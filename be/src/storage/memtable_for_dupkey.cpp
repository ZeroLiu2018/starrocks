// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "memtable_for_dupkey.h"

namespace starrocks::vectorized {

MemTableForDupKey::MemTableForDupKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                                     const std::vector<SlotDescriptor*>* slot_descs, RowsetWriter* rowset_writer,
                                     MemTracker* mem_tracker)
        : MemTable(tablet_id, tablet_schema, slot_descs, rowset_writer, mem_tracker) {}

MemTableForDupKey::MemTableForDupKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer,
                                     int64_t max_buffer_size, MemTracker* mem_tracker)
        : MemTable(tablet_id, schema, rowset_writer, max_buffer_size, mem_tracker) {}

Status MemTableForDupKey::finalize_impl() {
    _sort(true);
    return Status::OK();
}

bool MemTableForDupKey::_insert_full_callback() {
    return MemTable::_insert_full_callback();
}

size_t MemTableForDupKey::memory_usage() const {
    return MemTable::memory_usage();
}

size_t MemTableForDupKey::write_buffer_size() const {
    return MemTable::write_buffer_size();
}

} // namespace starrocks::vectorized