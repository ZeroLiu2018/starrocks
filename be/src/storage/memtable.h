// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <ostream>

#include "column/chunk.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/chunk_aggregator.h"
#include "storage/olap_define.h"

namespace starrocks {

class RowsetWriter;
class SlotDescriptor;
class TabletSchema;

namespace vectorized {

class MemTable {
public:
    // Disable copy/move ctor and assignment.
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    MemTable(MemTable&&) = delete;
    MemTable& operator=(MemTable&&) = delete;

    static std::unique_ptr<MemTable> create_memtable(int64_t tablet_id, const TabletSchema* tablet_schema,
                                                     const std::vector<SlotDescriptor*>* slot_descs,
                                                     RowsetWriter* rowset_writer, MemTracker* mem_tracker);

    MemTable(int64_t tablet_id, const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
             RowsetWriter* rowset_writer, MemTracker* mem_tracker);

    MemTable(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
             MemTracker* mem_tracker);

    virtual ~MemTable() = default;

    // return true suggests caller should flush this memory table
    bool insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size);

    Status flush();

    Status finalize();

    int64_t tablet_id() const { return _tablet_id; }

    // the total memory used (contain tmp chunk and aggregator chunk)
    virtual size_t memory_usage() const = 0;
    MemTracker* mem_tracker() { return _mem_tracker; }

    // buffer memory usage for write segment
    virtual size_t write_buffer_size() const = 0;

    bool is_full() const;

protected:
    virtual Status finalize_impl() = 0;
    virtual bool _insert_full_callback() = 0;

    void _sort(bool is_final);

    Schema _vectorized_schema;
    ChunkPtr _chunk;
    ChunkPtr _result_chunk;

    // memory usage and bytes usage calculation cost of object column is high,
    // so cache calculated memory usage and bytes usage to avoid repeated calculation.
    size_t _chunk_memory_usage = 0;
    size_t _chunk_bytes_usage = 0;

    // the slot in _slot_descs are in order of tablet's schema
    const std::vector<SlotDescriptor*>* _slot_descs;

    std::unique_ptr<Column> _deletes;

private:
    void _sort_column_inc();
    void _append_to_sorted_chunk(Chunk* src, Chunk* dest, bool is_final);

    vector<uint8_t> _result_deletes;

    // for sort by columns
    SmallPermutation _permutations;
    std::vector<uint32_t> _selective_values;

    int64_t _tablet_id;
    const TabletSchema* _tablet_schema;

    KeysType _keys_type;

    RowsetWriter* _rowset_writer;

    bool _use_slot_desc = true;
    int64_t _max_buffer_size = config::write_buffer_size;

    // memory statistic
    MemTracker* _mem_tracker = nullptr;
};

} // namespace vectorized

inline std::ostream& operator<<(std::ostream& os, const vectorized::MemTable& table) {
    os << "MemTable(addr=" << &table << ", tablet=" << table.tablet_id() << ", mem=" << table.memory_usage();
    return os;
}
} // namespace starrocks
