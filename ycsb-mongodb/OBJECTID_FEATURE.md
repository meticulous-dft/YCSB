# MongoDB ObjectId Support

This MongoDB YCSB client supports using MongoDB's native ObjectId for the `_id` field during the load phase for realistic data generation.

## Configuration

To use ObjectId during load phase, add this property:

```
mongodb.useObjectId=true
```

**Important**: This only works during the load phase (`-load` flag). The benchmark phase (`-t` flag) will always use YCSB string keys. If you load data with ObjectId mode, you cannot benchmark that data with YCSB operations.

## How it Works

### Default Behavior (mongodb.useObjectId=false or not set)
- Documents use YCSB-generated string keys as `_id` (e.g., "user123")
- Works for both load and benchmark phases
- All operations (insert, read, update, delete, scan) work with these string keys

### ObjectId Mode (mongodb.useObjectId=true during load)
- **Load phase**: Documents get MongoDB auto-generated ObjectIds as `_id`, YCSB keys stored in `ycsb_key` field
- **Benchmark phase**: Uses YCSB string keys and cannot read ObjectId-loaded data
- This mode is purely for realistic data generation, not for benchmarking

## Example Document Structure

### Default Mode (or benchmark phase)
```json
{
  "_id": "user123",
  "field0": "data...",
  "field1": "data..."
}
```

### ObjectId Mode (load phase only)
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "ycsb_key": "user123",
  "field0": "data...",
  "field1": "data..."
}
```

## Benefits

- Uses MongoDB's native ObjectId format for `_id` field during data generation
- ObjectIds provide better performance for sharding and indexing
- ObjectIds include timestamp information
- More realistic for MongoDB applications
- Perfect for load generation scenarios where you just need realistic data

## Limitations

- Only works during load phase - benchmark phase always uses YCSB string keys
- **Cannot benchmark ObjectId-loaded data** - YCSB benchmark operations cannot read documents loaded with ObjectId mode
- Results are not deterministic across load runs (ObjectIds are auto-generated)
- Slightly larger document size due to additional `ycsb_key` field when ObjectId mode is used
- Purely for data generation - if you need to benchmark, load data without ObjectId mode

## Usage Examples

### Default Behavior (YCSB string keys)

```bash
# Load data with YCSB string keys
./bin/ycsb load mongodb -P workloads/workloada

# Run benchmark with YCSB string keys (can read the loaded data)
./bin/ycsb run mongodb -P workloads/workloada
```

### ObjectId Mode (Load generation only)

```bash
# Load realistic data with MongoDB ObjectIds (for data generation)
./bin/ycsb load mongodb -P workloads/workloada -p mongodb.useObjectId=true

# Note: You CANNOT benchmark this ObjectId-loaded data with YCSB
# The benchmark phase will not be able to find the documents
```

### Separate Load and Benchmark Data

```bash
# Option 1: Load data for benchmarking (without ObjectId)
./bin/ycsb load mongodb -P workloads/workloada
./bin/ycsb run mongodb -P workloads/workloada

# Option 2: Load realistic data for other purposes (with ObjectId)
./bin/ycsb load mongodb -P workloads/workloada -p mongodb.useObjectId=true -p mongodb.collection=realistic_data
```
