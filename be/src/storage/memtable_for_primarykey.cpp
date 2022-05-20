// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "memtable_for_primarykey.h"

#include "storage/primary_key_encoder.h"

namespace starrocks::vectorized {
// TODO(cbl): move to common space latter
static const string LOAD_OP_COLUMN = "__op";
static const size_t kPrimaryKeyLimitSize = 128;

MemTableForPrimaryKey::MemTableForPrimaryKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                                             const std::vector<SlotDescriptor*>* slot_descs,
                                             RowsetWriter* rowset_writer, MemTracker* mem_tracker)
        : MemTableForAggregateKey(tablet_id, tablet_schema, slot_descs, rowset_writer, mem_tracker) {
    if (_slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        // load slots have __op field, so add to _vectorized_schema
        auto op_column = std::make_shared<starrocks::vectorized::Field>((ColumnId)-1, LOAD_OP_COLUMN,
                                                                        FieldType::OLAP_FIELD_TYPE_TINYINT, false);
        op_column->set_aggregate_method(OLAP_FIELD_AGGREGATION_REPLACE);
        _vectorized_schema.append(op_column);
        _has_op_slot = true;
    }
}

MemTableForPrimaryKey::MemTableForPrimaryKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer,
                                             int64_t max_buffer_size, MemTracker* mem_tracker)
        : MemTableForAggregateKey(tablet_id, schema, rowset_writer, max_buffer_size, mem_tracker) {}

Status MemTableForPrimaryKey::finalize_impl() {
    // TODO 这三行感觉统一 defer 处理更好，放在里边太隐秘了
//    _aggregator.reset();
//    _aggregator_memory_usage = 0;
//    _aggregator_bytes_usage = 0;
    RETURN_IF_ERROR(MemTableForAggregateKey::finalize_impl());
    if (PrimaryKeyEncoder::encode_exceed_limit(_vectorized_schema, *_result_chunk.get(), 0, _result_chunk->num_rows(),
                                               kPrimaryKeyLimitSize)) {
        return Status::Cancelled("primary key size exceed the limit.");
    }

    if (_has_op_slot) {
        // TODO(cbl): mem_tracker
        ChunkPtr upserts;
        RETURN_IF_ERROR(_split_upserts_deletes(_result_chunk, &upserts, &_deletes));
        if (_result_chunk != upserts) {
            _result_chunk = upserts;
        }
    }
    return Status::OK();
}

Status MemTableForPrimaryKey::_split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts,
                                                     std::unique_ptr<Column>* deletes) {
    size_t op_column_id = src->num_columns() - 1;
    auto op_column = src->get_column_by_index(op_column_id);
    src->remove_column_by_index(op_column_id);
    size_t nrows = src->num_rows();
    auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
    size_t ndel = 0;
    for (size_t i = 0; i < nrows; i++) {
        ndel += (ops[i] == TOpType::DELETE);
    }
    size_t nupsert = nrows - ndel;
    if (ndel == 0) {
        // no deletes, short path
        *upserts = src;
        return Status::OK();
    }
    vector<uint32_t> indexes[2];
    indexes[TOpType::UPSERT].reserve(nupsert);
    indexes[TOpType::DELETE].reserve(ndel);
    for (uint32_t i = 0; i < nrows; i++) {
        // ops == 0: upsert  otherwise: delete
        indexes[ops[i] == TOpType::UPSERT ? TOpType::UPSERT : TOpType::DELETE].push_back(i);
    }
    *upserts = src->clone_empty_with_schema(nupsert);
    (*upserts)->append_selective(*src, indexes[TOpType::UPSERT].data(), 0, nupsert);
    if (!(*deletes)) {
        auto st = PrimaryKeyEncoder::create_column(_vectorized_schema, deletes);
        if (!st.ok()) {
            LOG(ERROR) << "create column for primary key encoder failed, schema:" << _vectorized_schema
                       << ", status:" << st.to_string();
            return st;
        }
    }
    if (*deletes == nullptr) {
        return Status::RuntimeError("deletes pointer is null");
    } else {
        (*deletes)->reset_column();
    }
    auto& delidx = indexes[TOpType::DELETE];
    PrimaryKeyEncoder::encode_selective(_vectorized_schema, *src, delidx.data(), delidx.size(), deletes->get());
    return Status::OK();
}

bool MemTableForPrimaryKey::_insert_full_callback() {
    return MemTableForAggregateKey::_insert_full_callback();
}

size_t MemTableForPrimaryKey::memory_usage() const {
    return MemTableForAggregateKey::memory_usage();
}

size_t MemTableForPrimaryKey::write_buffer_size() const {
    return MemTableForAggregateKey::write_buffer_size();
}

} // namespace starrocks::vectorized