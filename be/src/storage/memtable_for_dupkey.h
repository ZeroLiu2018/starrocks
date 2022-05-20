// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include "memtable.h"

namespace starrocks {

namespace vectorized {

class MemTableForDupKey : public MemTable {
public:
    MemTableForDupKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                      const std::vector<SlotDescriptor*>* slot_descs, RowsetWriter* rowset_writer,
                      MemTracker* mem_tracker);
    MemTableForDupKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                      MemTracker* mem_tracker);

    ~MemTableForDupKey() override = default;

public:
    size_t memory_usage() const;
    size_t write_buffer_size() const;

private:
    Status finalize_impl() override;
    bool _insert_full_callback() override;
};

} // namespace vectorized

} // namespace starrocks