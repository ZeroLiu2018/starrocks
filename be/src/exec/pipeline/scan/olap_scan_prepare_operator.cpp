// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/olap_scan_prepare_operator.h"

#include "exec/vectorized/olap_scan_node.h"

namespace starrocks::pipeline {

/// OlapScanPrepareOperator
OlapScanPrepareOperator::OlapScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name,
                                                 int32_t plan_node_id, int32_t driver_sequence, OlapScanContextPtr ctx)
        : SourceOperator(factory, id, name, plan_node_id, driver_sequence), _ctx(std::move(ctx)) {
    _ctx->ref();
}

Status OlapScanPrepareOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    return _ctx->prepare(state);
}

void OlapScanPrepareOperator::close(RuntimeState* state) {
    _ctx->unref(state);
    SourceOperator::close(state);
}

bool OlapScanPrepareOperator::has_output() const {
    return !is_finished();
}

bool OlapScanPrepareOperator::is_finished() const {
    return _ctx->is_prepare_finished() || _ctx->is_finished();
}

StatusOr<vectorized::ChunkPtr> OlapScanPrepareOperator::pull_chunk(RuntimeState* state) {
    Status status = _ctx->parse_conjuncts(state, runtime_in_filters(), runtime_bloom_filters());

    _ctx->set_prepare_finished();
    if (!status.ok()) {
        _ctx->set_finished();
        return status;
    }

    return nullptr;
}

/// OlapScanPrepareOperatorFactory
OlapScanPrepareOperatorFactory::OlapScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, OlapScanContextPtr ctx)
        : SourceOperatorFactory(id, "olap_scan_prepare", plan_node_id), _ctx(std::move(ctx)) {}

Status OlapScanPrepareOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));

    const auto& conjunct_ctxs = _ctx->scan_node()->conjunct_ctxs();
    auto* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_ctx->scan_node());
    const auto& tolap_scan_node = olap_scan_node->thrift_olap_scan_node();
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(tolap_scan_node.tuple_id);

    vectorized::DictOptimizeParser::rewrite_descriptor(state, conjunct_ctxs, tolap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));
    return Status::OK();
}

OperatorPtr OlapScanPrepareOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<OlapScanPrepareOperator>(this, _id, _name, _plan_node_id, driver_sequence, _ctx);
}

} // namespace starrocks::pipeline
