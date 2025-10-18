# Aerospike Interactive CLI Client - Experimental

A powerful command-line interface for interacting with Aerospike databases. This tool provides an interactive prompt with command history, full CRUD operations, secondary index management, UDF support, comprehensive load testing capabilities, and advanced performance testing features.

## Features

- **Interactive Prompt** with command history (up/down arrow navigation)
- **CRUD Operations** - Create, Read, Update, Delete records
- **Secondary Index Management** - Create, show, and drop indexes
- **Query Support** - Query by secondary indexes
- **Batch Operations** - Batch reads and writes
- **UDF Support** - Register, list, execute, and remove User Defined Functions
- **Load Testing** - Comprehensive distributed workload testing (NEW!)
  - Distributed read/write testing across multiple keys
  - Batch read/write performance testing
  - Configurable concurrency, duration, and rate limiting
  - Detailed latency percentiles (P50, P95, P99, Max)
- **Hot Key Testing** - Single key contention testing
- **Advanced Operations** - Delete by digest, scan-touch, debug record metadata
- **Configurable Policies** - Comprehensive policy controls for read/write operations
- **Debug Mode** - Detailed error codes and messages
- **Dynamic Configuration** - Change namespace/set on the fly
- **Human-Readable Timestamps** - Automatic conversion of Aerospike epoch times

## Prerequisites

- Go 1.18 or higher
- Access to an Aerospike server
- Aerospike Go client library v7
- readline library for command history

## Installation

### 1. Clone or download the source code

Save the `main.go` file to your workspace.

### 2. Initialize Go module (if not already done)

```bash
go mod init aerospike-cli
```

### 3. Install dependencies

```bash
go get github.com/aerospike/aerospike-client-go/v7
go get github.com/chzyer/readline
```

### 4. Build the application

```bash
go build -o aerospike-cli main.go
```

## Usage

### Starting the Client

#### Basic Connection (localhost)

```bash
./aerospike-cli
```

#### Connect to Remote Server

```bash
./aerospike-cli -h 192.168.1.100 -p 3000 -n myNamespace -s mySet
```

#### Enable Debug Mode

```bash
./aerospike-cli -h localhost -p 3000 -d
```

#### With Custom Policies

```bash
./aerospike-cli --socket-timeout 5000 --max-retries 3 --connect-timeout 2000
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-h, --host` | Aerospike host or IP address | localhost |
| `-p, --port` | Aerospike port | 3000 |
| `-n, --namespace` | Default namespace | test |
| `-s, --set` | Default set | (empty) |
| `-d, --debug` | Enable debug mode | false |
| `--socket-timeout` | Socket timeout in milliseconds | 30000 |
| `--total-timeout` | Total timeout in milliseconds | 0 |
| `--max-retries` | Maximum number of retries | 2 |
| `--connect-timeout` | Connect timeout in milliseconds | 1000 |
| `--help` | Display help message | - |

## Interactive Commands

Once connected, you'll see the interactive prompt:

```
aerospike>
```

### Basic CRUD Operations

#### Insert/Update a Record (PUT)

```bash
aerospike> put user1 name=John age=30 city=NYC
```

#### Read a Record (GET)

```bash
aerospike> get user1
```

The output includes:
- Key value
- 20-byte digest (hexadecimal)
- Partition ID
- Generation and expiration (with human-readable date/time and TTL remaining)
- All bins with their values and types

Example output:
```
Key: user1
Digest: 0a1b2c3d4e5f6789abcdef0123456789abcdef01
Partition ID: 1523
Generation: 2, Expiration: 439467349 (2023-12-15 10:22:29 UTC, TTL: 5d 3h 45m 20s)
Bins:
  name: John (string)
  age: 30 (int64)
```

#### Delete a Record

```bash
aerospike> delete user1
```

#### Delete by Digest

Delete a record using its 20-byte digest (useful when you don't have the original key):

```bash
aerospike> delete-digest 0a1b2c3d4e5f6789abcdef0123456789abcdef01
```

The digest should be provided as a 40-character hexadecimal string. Spaces, colons, and hyphens are automatically removed.

#### Debug Record Metadata

Retrieve detailed record metadata without fetching bin data:

```bash
aerospike> debug-record-meta 0a1b2c3d4e5f6789abcdef0123456789abcdef01
```

This command returns:
- Generation number
- Void time (expiration)
- Last update time (with human-readable conversion)
- Number of bins
- Set name
- **XDR write flag** - Whether record needs XDR replication
- **XDR tombstone** - XDR tombstone status
- **XDR NSUP tombstone** - NSUP tombstone status
- **Tombstone flag** - Whether record is marked for deletion
- **Cenotaph flag** - Cenotaph status
- **Replication state** - Replication state
- **Key stored** - Whether user key is stored
- Partition ID and replica index
- Tree ID and reference count
- Storage information (rblock-id, n-rblocks, file-id)

### Secondary Index Operations

#### Create a Secondary Index

Create a numeric index:
```bash
aerospike> create-index idx_age age numeric
```

Create a string index:
```bash
aerospike> create-index idx_name name string
```

#### Show All Indexes

List all secondary indexes with their details:
```bash
aerospike> show-indexes
```

#### Drop an Index

```bash
aerospike> drop-index idx_age
```

### Query Operations

#### Query by Secondary Index

Query records where age equals 30:
```bash
aerospike> query age 30
```

Query records by name:
```bash
aerospike> query name John
```

#### Scan All Records

Scan all records in the current namespace/set:
```bash
aerospike> scan
```

#### Scan and Touch Records

Touch all records in the namespace/set, resetting their TTL to the namespace default:
```bash
aerospike> scan-touch
```

This command:
- Scans all records in the current namespace/set
- Touches each record with TTL=-2 (resets to namespace default TTL)
- Shows progress every 1000 records
- Requires confirmation before proceeding
- Displays total touched records and any errors

## Load Testing (Distributed Workloads)

The CLI includes comprehensive load testing features for testing distributed workloads across multiple keys. These commands are ideal for:
- Measuring cluster throughput capacity
- Testing performance under realistic workloads
- Identifying bottlenecks
- Capacity planning
- Performance regression testing

### Load Test: Distributed Reads

Test read performance across multiple keys with configurable concurrency and rate limiting.

```bash
aerospike> loadtest-read [keyPrefix] [numKeys] [threads] [duration] [rate]
```

**Parameters:**
- `keyPrefix` - Prefix for test keys (default: `loadtest`)
- `numKeys` - Number of unique keys in the test dataset (default: `1000`)
- `threads` - Number of concurrent worker threads (default: `50`)
- `duration` - Test duration in seconds (default: `60`)
- `rate` - Target operations per second per thread (default: `100`, use `0` for unlimited)

**Examples:**

Basic read load test with defaults:
```bash
aerospike> loadtest-read
```

High-volume test with 10,000 keys, 100 threads, 30 seconds:
```bash
aerospike> loadtest-read mytest 10000 100 30 500
```

Maximum throughput test (unlimited rate):
```bash
aerospike> loadtest-read perftest 5000 200 60 0
```

**Output:**
```
========================================
      Load Test - Distributed Reads
========================================
Key Prefix:  loadtest
Num Keys:    1000
Threads:     50
Duration:    60 seconds
Target Rate: 100 ops/sec per thread
Total Rate:  ~5000 ops/sec
========================================

Preparing test data... .......... Done!

Starting load test in 3 seconds...

Running... (updates every 5 seconds)
----------------------------------------
[5s] Ops: 24850 | Success: 24850 | Errors: 0 | Timeouts: 0 | Rate: 4970 ops/sec
[10s] Ops: 49900 | Success: 49900 | Errors: 0 | Timeouts: 0 | Rate: 4990 ops/sec
...

========================================
      READ Load Test Complete
========================================
Duration:        60.02 seconds
Total Ops:       299500
Successful:      299500 (100.00%)
Errors:          0 (0.00%)
Timeouts:        0 (0.00%)
Average Rate:    4990 ops/sec

Latency Distribution (microseconds):
  P50:  245 µs (0.25 ms)
  P95:  890 µs (0.89 ms)
  P99:  1450 µs (1.45 ms)
  Max:  3200 µs (3.20 ms)
========================================
```

### Load Test: Distributed Writes

Test write performance across multiple keys with the same configurability.

```bash
aerospike> loadtest-write [keyPrefix] [numKeys] [threads] [duration] [rate]
```

**Parameters:** Same as `loadtest-read`

**Examples:**

Basic write load test:
```bash
aerospike> loadtest-write
```

High-concurrency write test:
```bash
aerospike> loadtest-write writetest 5000 100 60 200
```

Stress test with unlimited writes:
```bash
aerospike> loadtest-write stress 10000 200 30 0
```

**Features:**
- Each write includes unique data: counter, worker_id, timestamp, and data fields
- Distributed across the key range to avoid hot key contention
- Rate limiting per thread for controlled load
- Real-time progress updates every 5 seconds

### Load Test: Batch Reads

Test batch read performance by reading multiple keys in each operation.

```bash
aerospike> loadtest-batch-read [keyPrefix] [numKeys] [batchSize] [threads] [duration] [rate]
```

**Parameters:**
- `keyPrefix` - Prefix for test keys (default: `loadtest`)
- `numKeys` - Number of unique keys in dataset (default: `1000`)
- `batchSize` - Number of keys per batch request (default: `10`)
- `threads` - Number of concurrent workers (default: `50`)
- `duration` - Test duration in seconds (default: `60`)
- `rate` - Target batch operations per second per thread (default: `10`)

**Examples:**

Basic batch read test (10 keys per batch):
```bash
aerospike> loadtest-batch-read
```

Large batch test (100 keys per batch):
```bash
aerospike> loadtest-batch-read batchtest 10000 100 50 60 5
```

Small batches, high rate:
```bash
aerospike> loadtest-batch-read smallbatch 5000 5 100 30 50
```

**Note:** The effective read rate is `batchSize × rate × threads`, so batch operations can achieve very high individual record read rates.

### Load Test: Batch Writes

Test batch write performance by writing multiple records in rapid succession.

```bash
aerospike> loadtest-batch-write [keyPrefix] [numKeys] [batchSize] [threads] [duration] [rate]
```

**Parameters:** Same as `loadtest-batch-read`

**Examples:**

Basic batch write test:
```bash
aerospike> loadtest-batch-write
```

High-volume batch writes:
```bash
aerospike> loadtest-batch-write bulkload 20000 50 100 120 20
```

**Features:**
- Writes multiple records in sequence per batch operation
- Each record gets unique data (counter, worker_id, batch_id, timestamp, data)
- Measures end-to-end batch write latency
- Useful for testing bulk loading scenarios

### Understanding Load Test Metrics

**Operation Counts:**
- **Total Ops** - Total operations attempted
- **Successful** - Operations completed successfully
- **Errors** - Operations failed with errors (non-timeout)
- **Timeouts** - Operations that exceeded timeout threshold

**Rate Metrics:**
- **Average Rate** - Total operations per second across all threads
- For batch operations, note the difference between batch operations/sec and individual record operations/sec

**Latency Percentiles:**
- **P50 (Median)** - 50% of operations completed in this time or less
- **P95** - 95% of operations completed in this time or less
- **P99** - 99% of operations completed in this time or less
- **Max** - Slowest operation in the test

## Hot Key Testing (Single Key Contention)

### Hot Key Read Workload (hotget)

Generate a high-rate read workload on a single key to test read performance and contention:

```bash
aerospike> hotget <key> [connections] [duration] [rate]
```

**Parameters:**
- `key` - The key to read repeatedly (required)
- `connections` - Number of concurrent connections (default: 100)
- `duration` - Test duration in seconds (default: 60)
- `rate` - Target ops/sec per connection (default: 1000, 0=unlimited)

**Examples:**
```bash
# Default: 100 connections, 60 seconds, 1000 ops/sec per connection
aerospike> hotget user1

# High load: 500 connections, 30 seconds, unlimited rate
aerospike> hotget user1 500 30 0

# Moderate: 200 connections, 120 seconds, 5000 ops/sec per connection
aerospike> hotget user1 200 120 5000
```

**Use cases:**
- Test server read throughput
- Test connection limits
- Identify timeout thresholds under load
- Stress test read replicas
- Network performance testing

### Hot Key Write Workload (hotput)

Generate a high-rate write workload on a single key to test write contention:

```bash
aerospike> hotput <key> [connections] [duration] [rate]
```

**Parameters:** Same as `hotget`

**Examples:**
```bash
# Default: 100 connections, 60 seconds, 1000 ops/sec per connection
aerospike> hotput testkey

# High contention: 500 connections, 30 seconds, unlimited rate
aerospike> hotput testkey 500 30 0
```

**Features:**
- Each write includes unique data (counter, worker_id, timestamp)
- Tests write lock contention
- Shows final generation count
- Displays final record state after completion

**Use cases:**
- Test write contention handling
- Test generation counter behavior
- Identify write bottlenecks
- Stress test replication under write load
- Network saturation testing

### Comparison: Hot Key vs Load Testing

**Hot Key Testing (`hotget`, `hotput`):**
- Tests single key contention
- All threads access the same record
- Identifies lock contention and maximum single-key throughput
- Use for: Hot key scenarios, cache testing, contention analysis

**Load Testing (`loadtest-*`):**
- Tests distributed workloads
- Threads access different keys randomly
- Measures cluster throughput capacity
- Use for: Capacity planning, performance baselines, realistic workloads

## UDF (User Defined Functions)

### Register a UDF Module

Register a Lua UDF file:
```bash
aerospike> register-udf /path/to/myudf.lua
```

Register with custom server filename:
```bash
aerospike> register-udf /path/to/myudf.lua custom_name.lua
```

### List Registered UDFs

View all registered UDF modules:
```bash
aerospike> list-udfs
```

### Execute UDF

The `execute-udf` command supports multiple execution modes with SQL-like syntax:

**Execute on a single record (returns result):**
```bash
aerospike> execute-udf myudf.increment(salary,1000) ON test.users WHERE PK = user1
```

**Execute on query results (background operation):**
```bash
aerospike> execute-udf myudf.process(status,active) ON test.users WHERE age = 30
```

**Execute on range query (background operation):**
```bash
aerospike> execute-udf myudf.adjust(field,value) ON test.users WHERE age BETWEEN 25 AND 35
```

**Execute on all records in namespace/set (background operation):**
```bash
aerospike> execute-udf myudf.updateAll(flag,true) ON test.users
```

### Remove a UDF Module

Remove a registered UDF:
```bash
aerospike> remove-udf myudf.lua
```

## Batch Operations

### Batch Write

Write multiple records at once:
```bash
aerospike> batch-put user1:name=Alice,age=25 user2:name=Bob,age=30 user3:name=Charlie,age=35
```

### Batch Read

Read multiple records at once:
```bash
aerospike> batch-get user1 user2 user3
```

## Configuration Commands

### View Current Configuration

```bash
aerospike> config show
```

Displays all current settings organized by category.

### Change Configuration at Runtime

```bash
aerospike> config set <parameter> <value>
```

### Switch Namespace/Set

Change namespace:
```bash
aerospike> use myNamespace
```

Change namespace and set:
```bash
aerospike> use myNamespace mySet
```

## Load Testing Best Practices

### 1. Data Preparation
- Load tests automatically prepare test data before starting
- For batch-read and read tests, data is created upfront
- Allow time for data preparation to complete

### 2. Choosing Thread Count
- Start with 50 threads and adjust based on results
- More threads = more load, but also more client overhead
- Monitor both client and server CPU usage

### 3. Rate Limiting
- Use rate limiting (`rate` parameter) to test specific throughput targets
- Set `rate=0` for maximum throughput testing
- Rate is per-thread, so total rate = rate × threads

### 4. Test Duration
- Use 60+ seconds for stable measurements
- Shorter tests (30s) for quick validation
- Longer tests (300s+) for stress testing and endurance

### 5. Key Distribution
- More keys (`numKeys`) reduces hot key contention
- Fewer keys tests contention handling
- Typical: 1000-10000 keys for distributed testing

### 6. Batch Sizing
- Larger batches = fewer network round trips, higher throughput
- Smaller batches = lower latency, more granular control
- Typical batch sizes: 10-100 keys

### 7. Progressive Load Testing
```bash
# Start light
aerospike> loadtest-read mytest 1000 10 30 50

# Increase threads
aerospike> loadtest-read mytest 1000 50 30 50

# Increase rate
aerospike> loadtest-read mytest 1000 50 30 200

# Maximum throughput
aerospike> loadtest-read mytest 1000 100 60 0
```

## Example Test Scenarios

### Scenario 1: Baseline Performance
```bash
# Test baseline read performance
aerospike> loadtest-read baseline 10000 50 120 100

# Test baseline write performance  
aerospike> loadtest-write baseline 10000 50 120 100
```

### Scenario 2: Batch Performance
```bash
# Compare different batch sizes
aerospike> loadtest-batch-read batch10 5000 10 50 60 20
aerospike> loadtest-batch-read batch50 5000 50 50 60 5
aerospike> loadtest-batch-read batch100 5000 100 50 60 2
```

### Scenario 3: Maximum Throughput
```bash
# Find maximum read throughput
aerospike> loadtest-read maxread 5000 200 60 0

# Find maximum write throughput
aerospike> loadtest-write maxwrite 5000 200 60 0
```

### Scenario 4: Latency Testing
```bash
# Low-rate test for latency measurement
aerospike> loadtest-read latency 1000 10 60 10
```

### Scenario 5: Stress Testing
```bash
# High concurrency stress test
aerospike> loadtest-write stress 10000 500 300 0
```

## Interpreting Results

### Good Performance Indicators
- Success rate > 99%
- Timeout rate < 1%
- P95 latency < 10ms for reads
- P95 latency < 20ms for writes
- Stable rate throughout test duration

### Performance Issues
- High timeout rate (>5%) - Check timeouts, network, or server capacity
- High error rate - Check server logs for errors
- P99 >> P95 - Indicates latency spikes or outliers
- Decreasing rate over time - Server may be overloaded

## Troubleshooting

### Load Tests

**Issue: Low throughput**
- Increase thread count
- Set rate to 0 (unlimited)
- Check client CPU usage
- Verify network bandwidth

**Issue: High timeouts**
- Increase socket-timeout: `config set socket-timeout 60000`
- Reduce load (fewer threads or lower rate)
- Check server capacity
- Verify network latency

**Issue: High latency**
- Check server CPU and memory
- Reduce concurrency
- Verify proper indexes exist for queries
- Check server configuration

**Issue: Client bottleneck**
- Run multiple CLI instances
- Reduce logging/debug output
- Use dedicated test machines

## Command History

The client maintains command history in `/tmp/.aerospike_history`. Use the **up/down arrow keys** to navigate through previously executed commands.

## License

This tool is provided as-is for use with Aerospike databases.

## Support

For issues related to:
- **Aerospike Server**: Visit [Aerospike Documentation](https://docs.aerospike.com/)
- **Aerospike Go Client**: Visit [Aerospike Go Client GitHub](https://github.com/aerospike/aerospike-client-go)
- **This CLI Tool**: Check command syntax with `help` command
