// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/memtable.h"

#include <memory>

#include "column/json_column.h"
#include "column/type_traits.h"
#include "common/logging.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/current_thread.h"
#include "runtime/primitive_type_infra.h"
#include "storage/chunk_helper.h"
#include "storage/memtable_for_aggregatekey.h"
#include "storage/memtable_for_dupkey.h"
#include "storage/memtable_for_primarykey.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/schema.h"
#include "util/orlp/pdqsort.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::vectorized {

MemTable::MemTable(int64_t tablet_id, const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
                   RowsetWriter* rowset_writer, MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(tablet_schema->keys_type()),
          _rowset_writer(rowset_writer),
          _mem_tracker(mem_tracker) {
    _vectorized_schema = std::move(ChunkHelper::convert_schema_to_format_v2(*tablet_schema));
}

MemTable::MemTable(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                   MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(std::move(schema)),
          _tablet_schema(nullptr),
          _slot_descs(nullptr),
          _keys_type(schema.keys_type()),
          _rowset_writer(rowset_writer),
          _use_slot_desc(false),
          _max_buffer_size(max_buffer_size),
          _mem_tracker(mem_tracker) {}

bool MemTable::insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunk == nullptr) {
        _chunk = ChunkHelper::new_chunk(_vectorized_schema, 0);
    }

    if (_use_slot_desc) {
        // For schema change, FE will construct a shadow column.
        // The shadow column is not exist in _vectorized_schema
        // So the chunk can only be accessed by the subscript
        // instead of the column name.
        for (int i = 0; i < _slot_descs->size(); ++i) {
            const ColumnPtr& src = chunk.get_column_by_slot_id((*_slot_descs)[i]->id());
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    } else {
        for (int i = 0; i < _vectorized_schema.num_fields(); i++) {
            const ColumnPtr& src = chunk.get_column_by_index(i);
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    }

    if (chunk.has_rows()) {
        _chunk_memory_usage += chunk.memory_usage() * size / chunk.num_rows();
        _chunk_bytes_usage += chunk.bytes_usage() * size / chunk.num_rows();
    }

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    if (is_full()) {
        return _insert_full_callback();
    }
}

bool MemTable::_insert_full_callback() {
    return true;
}

Status MemTable::finalize() {
    if (_chunk == nullptr) {
        return Status::OK();
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        RETURN_IF_ERROR(finalize_impl());
    }

    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

Status MemTable::flush() {
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_deletes) {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk_with_deletes(*_result_chunk, *_deletes));
        } else {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk(*_result_chunk));
        }
    }
    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    VLOG(1) << "memtable flush: " << duration_ns / 1000 << "us";
    return Status::OK();
}

size_t MemTable::memory_usage() const {
    size_t size = 0;

    // used for sort
    size += sizeof(PermutationItem) * _permutations.size();
    size += sizeof(uint32_t) * _selective_values.size();

    // _result_chunk is the final result before flush
    if (_result_chunk != nullptr && _result_chunk->num_rows() > 0) {
        size += _result_chunk->memory_usage();
    }

    return size + _chunk_memory_usage;
}

size_t MemTable::write_buffer_size() const {
    if (_chunk == nullptr) {
        return 0;
    }

    return _chunk_bytes_usage;
}

bool MemTable::is_full() const {
    return write_buffer_size() >= _max_buffer_size;
}

void MemTable::_sort(bool is_final) {
    SmallPermutation perm = create_small_permutation(_chunk->num_rows());
    std::swap(perm, _permutations);
    _sort_column_inc();

    if (is_final) {
        // No need to reserve, it will be reserve in IColumn::append_selective(),
        // Otherwise it will use more peak memory
        _result_chunk = _chunk->clone_empty_with_schema(0);
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), true);
        _chunk.reset();
    } else {
        _result_chunk = _chunk->clone_empty_with_schema();
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), false);
        _chunk->reset();
    }
    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;
}

void MemTable::_append_to_sorted_chunk(Chunk* src, Chunk* dest, bool is_final) {
    DCHECK_EQ(src->num_rows(), _permutations.size());
    permutate_to_selective(_permutations, &_selective_values);
    if (is_final) {
        dest->rolling_append_selective(*src, _selective_values.data(), 0, src->num_rows());
    } else {
        dest->append_selective(*src, _selective_values.data(), 0, src->num_rows());
    }
}

void MemTable::_sort_column_inc() {
    Columns columns;
    std::vector<int> sort_orders;
    std::vector<int> null_firsts;
    for (int i = 0; i < _vectorized_schema.num_key_fields(); i++) {
        columns.push_back(_chunk->get_column_by_index(i));
        // Ascending, null first
        sort_orders.push_back(1);
        null_firsts.push_back(-1);
    }

    Status st = stable_sort_and_tie_columns(false, columns, sort_orders, null_firsts, &_permutations);
    CHECK(st.ok());
}

#define CASE_CREATE_MEMTABLE(CASE_TYPE, MEMTABLE_TYPE)                                                            \
    case CASE_TYPE: {                                                                                             \
        return std::make_unique<MEMTABLE_TYPE>(tablet_id, tablet_schema, slot_descs, rowset_writer, mem_tracker); \
    }

std::unique_ptr<MemTable> MemTable::create_memtable(int64_t tablet_id, const TabletSchema* tablet_schema,
                                                    const std::vector<SlotDescriptor*>* slot_descs,
                                                    RowsetWriter* rowset_writer, MemTracker* mem_tracker) {
    auto keys_type = tablet_schema->keys_type();
    switch (keys_type) {
        CASE_CREATE_MEMTABLE(KeysType::DUP_KEYS, MemTableForDupKey)
        CASE_CREATE_MEMTABLE(KeysType::UNIQUE_KEYS, MemTableForAggregateKey)
        CASE_CREATE_MEMTABLE(KeysType::AGG_KEYS, MemTableForAggregateKey)
        CASE_CREATE_MEMTABLE(KeysType::PRIMARY_KEYS, MemTableForPrimaryKey)
    default:
        LOG(WARNING) << "memtable not support type: " << keys_type;
        return nullptr;
    }
}

} // namespace starrocks::vectorized
