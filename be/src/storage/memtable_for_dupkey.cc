// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "memtable_for_dupkey.h"

namespace starrocks::vectorized {

Status MemTableForDupKey::finalize_impl() {
    _sort(true);
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