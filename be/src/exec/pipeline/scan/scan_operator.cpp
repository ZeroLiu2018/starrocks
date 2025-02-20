// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "exec/vectorized/olap_scan_node.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

// ========== ScanOperator ==========

ScanOperator::ScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node)
        : SourceOperator(factory, id, scan_node->name(), scan_node->id(), driver_sequence),
          _scan_node(scan_node),
          _chunk_source_profiles(MAX_IO_TASKS_PER_OP),
          _is_io_task_running(MAX_IO_TASKS_PER_OP),
          _chunk_sources(MAX_IO_TASKS_PER_OP) {
    for (auto i = 0; i < MAX_IO_TASKS_PER_OP; i++) {
        _chunk_source_profiles[i] = std::make_shared<RuntimeProfile>(strings::Substitute("ChunkSource$0", i));
    }
}

Status ScanOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));

    if (_workgroup == nullptr) {
        DCHECK(_io_threads != nullptr);
        auto num_scan_operators = 1 + state->exec_env()->increment_num_scan_operators(1);
        if (num_scan_operators > _io_threads->get_queue_capacity()) {
            state->exec_env()->decrement_num_scan_operators(1);
            return Status::TooManyTasks(
                    strings::Substitute("num_scan_operators exceeds queue capacity($0) of pipeline_pool_thread",
                                        _io_threads->get_queue_capacity()));
        }
    }

    RETURN_IF_ERROR(do_prepare(state));

    return Status::OK();
}

void ScanOperator::close(RuntimeState* state) {
    if (_workgroup == nullptr) {
        state->exec_env()->decrement_num_scan_operators(1);
    }
    // for the running io task, we can't close its chunk sources.
    // After ScanOperator::close, these chunk sources are no longer meaningful,
    // just release resources by their default destructor
    for (size_t i = 0; i < _chunk_sources.size(); i++) {
        if (_chunk_sources[i] != nullptr && !_is_io_task_running[i]) {
            _chunk_sources[i]->close(state);
            _chunk_sources[i] = nullptr;
        }
    }

    _merge_chunk_source_profiles();
    do_close(state);
    Operator::close(state);
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
    }

    for (const auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && chunk_source->has_output()) {
            return true;
        }
    }

    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP) {
        return false;
    }

    // Because committing i/o task is trigger ONLY in pull_chunk,
    // return true if more i/o tasks can be committed.

    // Can pick up more morsels.
    if (!_morsel_queue->empty()) {
        return true;
    }

    // Can trigger_next_scan for the picked-up morsel.
    for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
        if (_chunk_sources[i] != nullptr && !_is_io_task_running[i] && _chunk_sources[i]->has_next_chunk()) {
            return true;
        }
    }

    return false;
}

bool ScanOperator::pending_finish() const {
    DCHECK(is_finished());
    return false;
}

bool ScanOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }

    // Any io task is running or needs to run.
    if (_num_running_io_tasks > 0 || !_morsel_queue->empty()) {
        return false;
    }

    for (const auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && (chunk_source->has_output() || chunk_source->has_next_chunk())) {
            return false;
        }
    }

    // This scan operator is finished, if no more io tasks are running
    // or need to run, and all the read chunks are consumed.
    return true;
}

Status ScanOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_try_to_trigger_next_scan(state));
    if (_workgroup != nullptr) {
        _workgroup->incr_period_ask_chunk_num(1);
    }

    for (auto& chunk_source : _chunk_sources) {
        if (chunk_source != nullptr && chunk_source->has_output()) {
            auto&& chunk = chunk_source->get_next_chunk_from_buffer();
            eval_runtime_bloom_filters(chunk.value().get());

            return std::move(chunk);
        }
    }

    return nullptr;
}

Status ScanOperator::_try_to_trigger_next_scan(RuntimeState* state) {
    if (_num_running_io_tasks >= MAX_IO_TASKS_PER_OP) {
        return Status::OK();
    }

    // Firstly, find the picked-up morsel, whose can commit an io task.
    for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
        if (_chunk_sources[i] != nullptr && !_is_io_task_running[i] && _chunk_sources[i]->has_next_chunk()) {
            RETURN_IF_ERROR(_trigger_next_scan(state, i));
        }
    }

    // Secondly, find the unused position of _chunk_sources to pick up a new morsel.
    if (!_morsel_queue->empty()) {
        for (int i = 0; i < MAX_IO_TASKS_PER_OP; ++i) {
            if (_chunk_sources[i] == nullptr || (!_is_io_task_running[i] && !_chunk_sources[i]->has_output())) {
                RETURN_IF_ERROR(_pickup_morsel(state, i));
            }
        }
    }

    return Status::OK();
}

// this is a more efficient way to check if a weak_ptr has been initialized
// ref: https://stackoverflow.com/a/45507610
// after compiler optimization, it generates far fewer instructions than std::weak_ptr::expired() and std::weak_ptr::lock()
// see: https://godbolt.org/z/16bWqqM5n
inline bool is_uninitialized(const std::weak_ptr<QueryContext>& ptr) {
    using wp = std::weak_ptr<QueryContext>;
    return !ptr.owner_before(wp{}) && !wp{}.owner_before(ptr);
}

Status ScanOperator::_trigger_next_scan(RuntimeState* state, int chunk_source_index) {
    _num_running_io_tasks++;
    _is_io_task_running[chunk_source_index] = true;

    bool offer_task_success = false;
    // to avoid holding mutex in bthread, we choose to initialize lazily here instead of in prepare
    if (is_uninitialized(_query_ctx)) {
        _query_ctx = state->exec_env()->query_context_mgr()->get(state->query_id());
    }
    if (_workgroup != nullptr) {
        workgroup::ScanTask task = workgroup::ScanTask(_workgroup, [wp = _query_ctx, this, state,
                                                                    chunk_source_index](int worker_id) {
            if (auto sp = wp.lock()) {
                {
                    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
                    size_t num_read_chunks = 0;
                    _chunk_sources[chunk_source_index]->buffer_next_batch_chunks_blocking_for_workgroup(
                            _buffer_size, state, &num_read_chunks, worker_id, _workgroup);
                    // TODO (by laotan332): More detailed information is needed
                    _workgroup->incr_period_scaned_chunk_num(num_read_chunks);
                    _workgroup->increment_real_runtime_ns(_chunk_sources[chunk_source_index]->last_spent_cpu_time_ns());

                    // for big query check
                    COUNTER_UPDATE(_total_cost_cpu_time_ns_counter,
                                   _chunk_sources[chunk_source_index]->last_spent_cpu_time_ns());
                    _last_scan_rows_num += _chunk_sources[chunk_source_index]->last_scan_rows_num();
                    _last_scan_bytes += _chunk_sources[chunk_source_index]->last_scan_bytes();
                }

                _num_running_io_tasks--;
                _is_io_task_running[chunk_source_index] = false;
            }
        });

        if (dynamic_cast<ConnectorScanOperator*>(this) != nullptr) {
            offer_task_success = ExecEnv::GetInstance()->hdfs_scan_executor()->submit(std::move(task));
        } else {
            offer_task_success = ExecEnv::GetInstance()->scan_executor()->submit(std::move(task));
        }
    } else {
        PriorityThreadPool::Task task;
        task.work_function = [wp = _query_ctx, this, state, chunk_source_index]() {
            if (auto sp = wp.lock()) {
                {
                    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
                    _chunk_sources[chunk_source_index]->buffer_next_batch_chunks_blocking(_buffer_size, state);
                    _last_scan_rows_num += _chunk_sources[chunk_source_index]->last_scan_rows_num();
                    _last_scan_bytes += _chunk_sources[chunk_source_index]->last_scan_bytes();
                }

                _num_running_io_tasks--;
                _is_io_task_running[chunk_source_index] = false;
            }
        };
        // TODO(by satanson): set a proper priority
        task.priority = 20;

        offer_task_success = _io_threads->try_offer(task);
    }

    if (offer_task_success) {
        _io_task_retry_cnt = 0;
    } else {
        _num_running_io_tasks--;
        _is_io_task_running[chunk_source_index] = false;
        // TODO(hcf) set a proper retry times
        LOG(WARNING) << "ScanOperator failed to offer io task due to thread pool overload, retryCnt="
                     << _io_task_retry_cnt;
        if (++_io_task_retry_cnt > 100) {
            return Status::RuntimeError("ScanOperator failed to offer io task due to thread pool overload");
        }
    }

    return Status::OK();
}

void ScanOperator::set_workgroup(starrocks::workgroup::WorkGroupPtr wg) {
    _workgroup = wg;
}

Status ScanOperator::_pickup_morsel(RuntimeState* state, int chunk_source_index) {
    DCHECK(_morsel_queue != nullptr);
    if (_chunk_sources[chunk_source_index] != nullptr) {
        _chunk_sources[chunk_source_index]->close(state);
        _chunk_sources[chunk_source_index] = nullptr;
    }

    auto maybe_morsel = _morsel_queue->try_get();
    if (maybe_morsel.has_value()) {
        auto morsel = std::move(maybe_morsel.value());
        DCHECK(morsel);
        _chunk_sources[chunk_source_index] = create_chunk_source(std::move(morsel), chunk_source_index);
        auto status = _chunk_sources[chunk_source_index]->prepare(state);
        if (!status.ok()) {
            _chunk_sources[chunk_source_index] = nullptr;
            _is_finished = true;
            return status;
        }
        RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
    }

    return Status::OK();
}

void ScanOperator::_merge_chunk_source_profiles() {
    std::vector<RuntimeProfile*> profiles(_chunk_source_profiles.size());
    for (auto i = 0; i < _chunk_source_profiles.size(); i++) {
        profiles[i] = _chunk_source_profiles[i].get();
    }
    RuntimeProfile::merge_isomorphic_profiles(profiles);

    RuntimeProfile* merged_profile = profiles[0];

    _unique_metrics->copy_all_info_strings_from(merged_profile);
    _unique_metrics->copy_all_counters_from(merged_profile);
}

// ========== ScanOperatorFactory ==========

ScanOperatorFactory::ScanOperatorFactory(int32_t id, ScanNode* scan_node)
        : SourceOperatorFactory(id, scan_node->name(), scan_node->id()), _scan_node(scan_node) {}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(do_prepare(state));

    return Status::OK();
}

OperatorPtr ScanOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return do_create(degree_of_parallelism, driver_sequence);
}

void ScanOperatorFactory::close(RuntimeState* state) {
    do_close(state);

    OperatorFactory::close(state);
}

// ====================

pipeline::OpFactories decompose_scan_node_to_pipeline(std::shared_ptr<ScanOperatorFactory> scan_operator,
                                                      ScanNode* scan_node, pipeline::PipelineBuilderContext* context) {
    OpFactories operators;

    auto& morsel_queues = context->fragment_context()->morsel_queues();
    auto source_id = scan_operator->plan_node_id();
    DCHECK(morsel_queues.count(source_id));
    auto& morsel_queue = morsel_queues[source_id];

    // ScanOperator's degree_of_parallelism is not more than the number of morsels
    // If table is empty, then morsel size is zero and we still set degree of parallelism to 1
    const auto degree_of_parallelism =
            std::min<size_t>(std::max<size_t>(1, morsel_queue->num_morsels()), context->degree_of_parallelism());
    scan_operator->set_degree_of_parallelism(degree_of_parallelism);

    operators.emplace_back(std::move(scan_operator));

    size_t limit = scan_node->limit();
    if (limit != -1) {
        operators.emplace_back(
                std::make_shared<pipeline::LimitOperatorFactory>(context->next_operator_id(), scan_node->id(), limit));
    }
    return operators;
}
} // namespace starrocks::pipeline
