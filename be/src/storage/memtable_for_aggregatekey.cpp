// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "memtable_for_aggregatekey.h"

#include "util/time.h"

namespace starrocks::vectorized {

MemTableForAggregateKey::MemTableForAggregateKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                                                 const std::vector<SlotDescriptor*>* slot_descs,
                                                 RowsetWriter* rowset_writer, MemTracker* mem_tracker)
        : MemTable(tablet_id, tablet_schema, slot_descs, rowset_writer, mem_tracker) {
    // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
    // which is not suitable for obtaining Chunk from ColumnPool,
    // otherwise it will take up a lot of memory and may not be released.
    _aggregator = MemTableAggregator(&_vectorized_schema);
}

MemTableForAggregateKey::MemTableForAggregateKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer,
                                                 int64_t max_buffer_size, MemTracker* mem_tracker)
        : MemTable(tablet_id, schema, rowset_writer, max_buffer_size, mem_tracker) {
    // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
    // which is not suitable for obtaining Chunk from ColumnPool,
    // otherwise it will take up a lot of memory and may not be released.
    _aggregator = MemTableAggregator(&_vectorized_schema);
}

void MemTableForAggregateKey::_merge() {
    if (_chunk == nullptr) {
        return;
    }

    int64_t t1 = MonotonicMicros();
    _sort(false);
    int64_t t2 = MonotonicMicros();
    _aggregator.do_aggregate(false, _result_chunk);
    int64_t t3 = MonotonicMicros();
    VLOG(1) << Substitute("memtable sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
    ++_merge_count;
}

void MemTableForAggregateKey::final_merge() {
    if (_chunk->num_rows() > 0) {
        // merge last undo merge
        _merge();
    }

    if (_merge_count > 1) {
        _chunk = _aggregator.aggregate_result();
        _aggregator.aggregate_reset();

        int64_t t1 = MonotonicMicros();
        _sort(true);
        int64_t t2 = MonotonicMicros();
        _aggregator.do_aggregate(true, _result_chunk);
        int64_t t3 = MonotonicMicros();
        VLOG(1) << Substitute("memtable final sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
    } else {
        // if there is only one data chunk and merge once,
        // no need to perform an additional merge.
        _chunk.reset();
        _result_chunk.reset();
    }
}

Status MemTableForAggregateKey::finalize_impl() {
    final_merge();

    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;

    _result_chunk = _aggregator.aggregate_result();

    _aggregator.reset();
    return Status::OK();
}

size_t MemTableForAggregateKey::memory_usage() const {
    return MemTable::memory_usage() + _aggregator.memory_usage();
}

size_t MemTableForAggregateKey::write_buffer_size() const {
    return MemTable::write_buffer_size() + _aggregator.bytes_usage();
}

bool MemTableForAggregateKey::_insert_full_callback() {
    size_t orig_bytes = write_buffer_size();
    _merge();
    size_t new_bytes = write_buffer_size();
    bool suggest_flush = false;
    if (new_bytes > orig_bytes * 2 / 3 && _merge_count <= 1) {
        // this means aggregate doesn't remove enough duplicate rows,
        // keep inserting into the buffer will cause additional sort&merge,
        // the cost of extra sort&merge is greater than extra flush IO,
        // so flush is suggested even buffer is not full
        suggest_flush = true;
    }
    return suggest_flush;
}

void MemTableAggregator::do_aggregate(bool is_final, ChunkPtr& result_chunk) {
    if (result_chunk == nullptr || result_chunk->num_rows() <= 0) {
        return;
    }

    DCHECK(result_chunk->num_rows() < INT_MAX);
    DCHECK(_aggregator->source_exhausted());

    _aggregator->update_source(result_chunk);

    DCHECK(_aggregator->is_do_aggregate());

    _aggregator->aggregate();
    _aggregator_memory_usage = _aggregator->memory_usage();
    _aggregator_bytes_usage = _aggregator->bytes_usage();

    // impossible finish
    DCHECK(!_aggregator->is_finish());
    DCHECK(_aggregator->source_exhausted());

    if (is_final) {
        result_chunk.reset();
    } else {
        result_chunk->reset();
    }
}

ChunkPtr MemTableAggregator::aggregate_result() {
    return _aggregator->aggregate_result();
}

void MemTableAggregator::aggregate_reset() {
    _aggregator->aggregate_reset();
}

void MemTableAggregator::reset() {
    _aggregator.reset();
    _aggregator_memory_usage = 0;
    _aggregator_bytes_usage = 0;
}

} // namespace starrocks::vectorized