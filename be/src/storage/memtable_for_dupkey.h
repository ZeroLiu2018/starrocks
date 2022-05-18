// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include "memtable.h"

namespace starrocks {

namespace vectorized {

class MemTableForDupKey : public MemTable {
public:
    MemTableForDupKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                      const std::vector<SlotDescriptor*>* slot_descs, RowsetWriter* rowset_writer,
                      MemTracker* mem_tracker)
            : MemTable(tablet_id, tablet_schema, slot_descs, rowset_writer, mem_trackert) {}
    MemTableForDupKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                      MemTracker* mem_tracker)
            : Memtable(tablet_id, schema, rowset_writer, max_buffer_size, mem_tracker) {}

    ~MemTableForDupKey() override = defualt;
public:
    size_t memory_usage() const;
    size_t write_buffer_size() const;
private:
    Status finalize_impl() override;
    bool _insert_full_callback() override;
};

} // namespace vectorized

} // namespace starrocks