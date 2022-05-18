// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include "memtable_for_aggregatekey.h"

namespace starrocks {

namespace vectorized {

class MemTableForPrimaryKey : public MemTableForAggregateKey {
public:
    MemTableForPrimaryKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                          const std::vector<SlotDescriptor*>* slot_descs, RowsetWriter* rowset_writer,
                          MemTracker* mem_tracker);
    MemTableForPrimaryKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                          MemTracker* mem_tracker);
    ~MemTableForPrimaryKey() override = default;

    size_t memory_usage() const override;

    size_t write_buffer_size() const override;

private:
    Status finalize_impl() override;
    bool _insert_full_callback() override;
    
    Status _split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, std::unique_ptr<Column>* deletes);
    bool _has_op_slot = false;
};

} // namespace vectorized

} // namespace starrocks