#pragma once
#include "memtable.h"

namespace starrocks {

namespace vectorized {

class MemTableAggregator {
public:
    MemTableAggregator() = default;
    MemTableAggregator(const Schema* schema) : _aggregator(std::make_unique<ChunkAggregator>(schema, 0, INT_MAX, 0)) {}
    ~MemTableAggregator() = default;
    MemTableAggregator(MemTableAggregator&&) = default;
    MemTableAggregator& operator=(MemTableAggregator&&) = default;

    void do_aggregate(bool is_final, ChunkPtr& result_chunk);
    ChunkPtr aggregate_result();
    void aggregate_reset();
    size_t memory_usage() const { return _aggregator_memory_usage; }
    size_t bytes_usage() const { return _aggregator_bytes_usage; }
    void reset();

    // Disable copy/move ctor and assignment.
    MemTableAggregator(const MemTableAggregator&) = delete;
    MemTableAggregator& operator=(const MemTableAggregator&) = delete;

private:
    // aggregate
    std::unique_ptr<ChunkAggregator> _aggregator;
    size_t _aggregator_memory_usage = 0;
    size_t _aggregator_bytes_usage = 0;
};

class MemTableForAggregateKey : public MemTable {
public:
    MemTableForAggregateKey(int64_t tablet_id, const TabletSchema* tablet_schema,
                            const std::vector<SlotDescriptor*>* slot_descs, RowsetWriter* rowset_writer,
                            MemTracker* mem_tracker);
    MemTableForAggregateKey(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer,
                            int64_t max_buffer_size, MemTracker* mem_tracker);

    ~MemTableForAggregateKey() override = default;

    size_t memory_usage() const override;

    size_t write_buffer_size() const override;

protected:
    Status finalize_impl() override;
    bool _insert_full_callback() override;

    void final_merge();
    // aggregate
    MemTableAggregator _aggregator;

private:
    void _merge();

    uint64_t _merge_count = 0;
};

} // namespace vectorized

} // namespace starrocks