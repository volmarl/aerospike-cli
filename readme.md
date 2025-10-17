# Aerospike Interactive CLI Client

A powerful command-line interface for interacting with Aerospike databases. This tool provides an interactive prompt with command history, full CRUD operations, secondary index management, UDF support, and advanced performance testing capabilities.

## Features

- **Interactive Prompt** with command history (up/down arrow navigation)
- **CRUD Operations** - Create, Read, Update, Delete records
- **Secondary Index Management** - Create, show, and drop indexes
- **Query Support** - Query by secondary indexes
- **Batch Operations** - Batch reads and writes
- **UDF Support** - Register, list, execute, and remove User Defined Functions
- **Advanced Operations** - Delete by digest, scan-touch, debug record metadata
- **Performance Testing** - Hot key read/write workload generation
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

**Example output:**
```
Record Metadata:
========================================
Digest (keyd): F49F36CF2DCA1A4259C571F448F62525C06A8B00
Namespace: test
Set: testset
----------------------------------------
Generation: 1
Void Time (Expiration): 0
Last Update Time: 439467349490 (2023-12-15 10:22:29 UTC)
Number of Bins: 2
Set Name: testset
----------------------------------------
Flags:
  XDR Write: 0
  XDR Tombstone: 0
  XDR NSUP Tombstone: 0
  Tombstone: 0
  Cenotaph: 0
  Replication State: 0
  Key Stored: 1
----------------------------------------
Index Information:
  Partition ID: 4084
  Replica Index: 1
  Tree ID: 1
  Reference Count: 0
========================================
```

**Use cases:**
- Debugging replication issues
- Verifying XDR replication status
- Checking tombstone records
- Understanding record metadata without reading bin data
- Troubleshooting data inconsistencies

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

This displays:
- Index name
- Namespace
- Set (if applicable)
- Bin name
- Data type (NUMERIC or STRING)
- State (RW = ready, WO = write-only during building)
- Index type (e.g., NONE for simple indexes)

**Example output:**
```
Secondary Indexes:
========================================

[1] Index: idx_age
    Namespace: test
    Set:       users
    Bin:       age
    Type:      NUMERIC
    State:     RW
    IndexType: NONE

[2] Index: idx_status
    Namespace: test
    Set:       (none)
    Bin:       status
    Type:      STRING
    State:     RW
    IndexType: NONE

Total indexes: 2
========================================
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

**Use cases:**
- Reset expiration times for all records after changing namespace TTL configuration
- Refresh records that are approaching expiration
- Bulk TTL management operations

**Example output:**
```
This will touch ALL records in namespace 'test', set 'users'
Each record's TTL will be reset to the namespace default (TTL=-2)
Are you sure you want to continue? (yes/no): yes

Starting scan-touch operation...
----------------------------------------
Touched 1000 records...
Touched 2000 records...
Touched 3000 records...

----------------------------------------
Scan-touch completed
  Records touched: 3450
  Errors: 0
```

### UDF (User Defined Functions)

#### Register a UDF Module

Register a Lua UDF file:
```bash
aerospike> register-udf /path/to/myudf.lua
```

Register with custom server filename:
```bash
aerospike> register-udf /path/to/myudf.lua custom_name.lua
```

#### List Registered UDFs

View all registered UDF modules:
```bash
aerospike> list-udfs
```

#### Execute UDF

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

**Syntax:**
```
execute-udf <module>.<function>(<arg1>,<arg2>,...) ON <namespace>[.<set>] [WHERE clause]

WHERE clause options:
  - WHERE PK = <key>                       (single record by primary key)
  - WHERE <bin> = <value>                  (query by indexed bin)
  - WHERE <bin> BETWEEN <lower> AND <upper> (range query)
  - (no WHERE clause)                      (all records - background scan)
```

#### Remove a UDF Module

Remove a registered UDF:
```bash
aerospike> remove-udf myudf.lua
```

### Batch Operations

#### Batch Write

Write multiple records at once:
```bash
aerospike> batch-put user1:name=Alice,age=25 user2:name=Bob,age=30 user3:name=Charlie,age=35
```

#### Batch Read

Read multiple records at once:
```bash
aerospike> batch-get user1 user2 user3
```

### Performance Testing

#### Hot Key Read Workload (hotget)

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

#### Hot Key Write Workload (hotput)

Generate a high-rate write workload on a single key to test write contention:

```bash
aerospike> hotput <key> [connections] [duration] [rate]
```

**Parameters:**
- `key` - The key to write repeatedly (required)
- `connections` - Number of concurrent connections (default: 100)
- `duration` - Test duration in seconds (default: 60)
- `rate` - Target ops/sec per connection (default: 1000, 0=unlimited)

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

### Configuration Commands

#### View Current Configuration

```bash
aerospike> config show
```

Displays all current settings organized by category:
- **Connection**: Host, port, namespace, set, debug mode
- **Timeout Policies**: Socket, total, and connect timeouts, max retries
- **Write Policies**: Record exists action, generation policy, expiration/TTL, durable delete, send key, commit level
- **Read Policies**: Read modes (AP/SC), replica policy

#### Change Configuration at Runtime

```bash
aerospike> config set <parameter> <value>
```

**Timeout Parameters:**
- `socket-timeout <ms>` - Socket timeout in milliseconds
- `total-timeout <ms>` - Total transaction timeout
- `max-retries <n>` - Maximum retry attempts
- `debug <true|false>` - Enable/disable debug mode

**Write Policy Parameters:**
- `record-exists-action <action>` - How to handle existing records
  - `UPDATE` - Create or update (default)
  - `UPDATE_ONLY` - Update only if exists
  - `REPLACE` - Replace entire record
  - `REPLACE_ONLY` - Replace only if exists
  - `CREATE_ONLY` - Create only if doesn't exist
- `generation-policy <policy>` - Generation checking for optimistic concurrency control
  - `NONE` - No generation check (default)
  - `EXPECT_GEN_EQUAL` - Expect exact generation match
  - `EXPECT_GEN_GT` - Expect generation greater than
- `generation <n>` - Expected generation number (use with generation-policy)
- `expiration <seconds>` or `ttl <seconds>` - Time-to-live
  - Positive number: TTL in seconds
  - `0`: Use namespace default (default)
  - `-1`: Never expire
  - `-2`: Reset to namespace default
- `durable-delete <true|false>` - Durable delete (commit to device)
- `send-key <true|false>` - Store user key on server (enables key retrieval)
- `commit-level <level>` - Commit level
  - `COMMIT_ALL` - Wait for all replicas (default)
  - `COMMIT_MASTER` - Wait for master only

**Read Policy Parameters:**
- `replica <policy>` - Which replica to read from
  - `SEQUENCE` - Try sequence of nodes (default)
  - `MASTER` - Always read from master
  - `MASTER_PROLES` - Distribute reads across master and proles
  - `RANDOM` - Distribute reads randomly
  - `PREFER_RACK` - Prefer local rack
- `read-mode-ap <mode>` - Availability mode for AP namespaces
  - `ONE` - Read from one replica (default)
  - `ALL` - Read from all replicas
- `read-mode-sc <mode>` - Consistency mode for SC namespaces
  - `SESSION` - Session consistency (default)
  - `LINEARIZE` - Linearizable reads
  - `ALLOW_REPLICA` - Allow replica reads
  - `ALLOW_UNAVAILABLE` - Allow unavailable reads

**Examples:**

```bash
# Enable durable delete for safe deletions
aerospike> config set durable-delete true

# Store user keys on server (useful for scans without digest)
aerospike> config set send-key true

# Set TTL to 1 hour for all writes
aerospike> config set ttl 3600

# Only update existing records
aerospike> config set record-exists-action UPDATE_ONLY

# Use generation checking for optimistic concurrency control
aerospike> config set generation-policy EXPECT_GEN_EQUAL
aerospike> config set generation 5

# Read from master only for strong consistency
aerospike> config set replica MASTER

# Set commit level to master only for faster writes
aerospike> config set commit-level COMMIT_MASTER
```

#### Switch Namespace/Set

Change namespace:
```bash
aerospike> use myNamespace
```

Change namespace and set:
```bash
aerospike> use myNamespace mySet
```

### Utility Commands

#### Get Help

```bash
aerospike> help
```

#### Exit the Client

```bash
aerospike> exit
```
or
```bash
aerospike> quit
```

## Command History

The client maintains command history in `/tmp/.aerospike_history`. Use the **up/down arrow keys** to navigate through previously executed commands.

## Data Type Handling

The client automatically parses and handles different data types:

- **Integers**: `42`, `100`, `-5`
- **Floats**: `3.14`, `2.5`
- **Booleans**: `true`, `false`
- **Strings**: Any value that doesn't match above types

Example:
```bash
aerospike> put record1 count=100 price=19.99 active=true name=Product
```

## Aerospike Epoch Times

Aerospike uses the **Citrusleaf epoch** which starts at **January 1, 2010 00:00:00 UTC** (not Unix epoch of 1970).

### Expiration Values:
- **0**: Record never expires
- **4294967295**: Record never expires (max uint32)
- **Positive number**: Seconds since January 1, 2010 when record expires

### Last Update Time (LUT):
- Stored in **milliseconds** since Citrusleaf epoch
- Automatically converted to readable format in output

Example conversions shown in CLI:
```
Expiration: 439467349 (2023-12-15 10:22:29 UTC, TTL: 5d 3h 45m 20s)
Last Update Time: 439467349490 (2023-12-15 10:22:29 UTC)
```

## Debug Mode

When debug mode is enabled (via `-d` flag or `config set debug true`), the client displays detailed error information including:

- Result codes
- Error messages
- InDoubt status for transactions

Example debug output:
```
ERROR: Put failed
  ResultCode: KEY_NOT_FOUND_ERROR
  Message: Key not found
  InDoubt: false
```

## Example Workflow

Here's a complete example workflow:

```bash
# Start the client
./aerospike-cli -h localhost -p 3000 -n test -d

# Create a secondary index on age
aerospike> create-index idx_age age numeric

# Insert some records
aerospike> put user1 name=Alice age=25 city=NYC salary=50000
aerospike> put user2 name=Bob age=30 city=LA salary=60000
aerospike> put user3 name=Charlie age=25 city=SF salary=55000

# Query by age
aerospike> query age 25

# Show all indexes
aerospike> show-indexes

# Register a UDF (assuming you have increment.lua)
aerospike> register-udf /path/to/increment.lua

# List registered UDFs
aerospike> list-udfs

# Execute UDF on a single record
aerospike> execute-udf increment.add_value(salary,5000) ON test.users WHERE PK = user1

# Execute UDF on query results (background operation)
aerospike> execute-udf increment.add_value(salary,2000) ON test.users WHERE age = 25

# Execute UDF on range (background operation)
aerospike> execute-udf increment.add_value(bonus,500) ON test.users WHERE age BETWEEN 25 AND 30

# Execute background UDF on all records
aerospike> execute-udf increment.add_value(bonus,1000) ON test.users

# Batch read
aerospike> batch-get user1 user2 user3

# Update a record
aerospike> put user1 name=Alice age=26 city=NYC

# Get record with full metadata
aerospike> get user1

# Debug record metadata by digest
aerospike> debug-record-meta 0a1b2c3d4e5f6789abcdef0123456789abcdef01

# Performance test - read workload
aerospike> hotget user1 100 30 1000

# Performance test - write workload
aerospike> hotput testkey 50 20 500

# Delete a record
aerospike> delete user3

# Scan all records
aerospike> scan

# Touch all records to reset TTL
aerospike> scan-touch

# Remove UDF
aerospike> remove-udf increment.lua

# Drop the index
aerospike> drop-index idx_age

# Exit
aerospike> exit
```

## Sample UDF Files

### Example 1: Simple Increment UDF (increment.lua)

```lua
-- Function to add a value to a bin
function add_value(rec, bin_name, value)
    if not aerospike:exists(rec) then
        return 0
    end
    
    local current = rec[bin_name] or 0
    rec[bin_name] = current + value
    
    aerospike:update(rec)
    return rec[bin_name]
end

-- Function to increment a counter
function increment_counter(rec, bin_name)
    return add_value(rec, bin_name, 1)
end
```

### Example 2: Record Processing UDF (process.lua)

```lua
-- Process record based on action
function processRecord(rec, action, bin_name, value)
    if not aerospike:exists(rec) then
        return nil
    end
    
    if action == "multiply" then
        rec[bin_name] = rec[bin_name] * value
    elseif action == "add" then
        rec[bin_name] = rec[bin_name] + value
    elseif action == "set" then
        rec[bin_name] = value
    end
    
    aerospike:update(rec)
    return rec[bin_name]
end

-- Update status for all records (for background scan)
function updateStatus(rec, status_bin, new_status)
    if not aerospike:exists(rec) then
        return nil
    end
    
    rec[status_bin] = new_status
    rec["last_updated"] = os.time()
    
    aerospike:update(rec)
    return 1
end

-- Get record info as a map
function getInfo(rec)
    local result = map()
    result["digest"] = record.digest(rec)
    result["gen"] = record.gen(rec)
    result["ttl"] = record.ttl(rec)
    return result
end
```

### Example 3: Aggregation UDF (aggregate.lua)

```lua
-- Stream UDF for aggregation
function sum_values(stream, bin_name)
    local function map_record(rec)
        return rec[bin_name] or 0
    end
    
    local function reduce_values(v1, v2)
        return v1 + v2
    end
    
    return stream : map(map_record) : reduce(reduce_values)
end

-- Count records matching a condition
function count_above_threshold(stream, bin_name, threshold)
    local function filter_record(rec)
        return rec[bin_name] > threshold
    end
    
    local function map_to_one(rec)
        return 1
    end
    
    local function sum(v1, v2)
        return v1 + v2
    end
    
    return stream : filter(filter_record) : map(map_to_one) : reduce(sum)
end
```

## Troubleshooting

### Connection Issues

If you cannot connect to the Aerospike server:

1. Verify the server is running: `asd status` or `systemctl status aerospike`
2. Check the host and port are correct
3. Ensure firewall rules allow connections on the Aerospike port
4. Try increasing the connect timeout: `--connect-timeout 5000`

### Query Failures

If queries fail:

1. Ensure a secondary index exists on the bin you're querying
2. Create the index using `create-index` command
3. Verify the index type matches the data type (numeric vs string)
4. Use `show-indexes` to verify index state is RW (read-write ready)

### UDF Issues

If UDF operations fail:

1. **Registration fails**: 
   - Verify the Lua file exists and is readable
   - Check for syntax errors in the Lua code
   - Ensure the file path is correct

2. **Execution fails**:
   - Verify the UDF is registered using `list-udfs`
   - Check that function name matches exactly (case-sensitive)
   - Ensure module name matches the registered filename
   - Verify arguments match the function signature

3. **Query-UDF returns no results**:
   - Make sure records match the query criteria
   - Verify the UDF function returns a value
   - Check that the UDF doesn't have runtime errors

### Timeout Errors

If you encounter timeout errors:

1. Increase socket timeout: `config set socket-timeout 60000`
2. Increase total timeout: `config set total-timeout 60000`
3. Check network connectivity and server load
4. For scan-touch operations on large datasets, timeouts are normal - the operation continues on the server

### Scan-Touch Operations

For large datasets:

1. **Progress monitoring** - Shows progress every 1000 records
2. **Errors are logged** - Individual record failures don't stop the scan
3. **Confirmation required** - Prevents accidental bulk operations
4. **Enable debug mode** - See detailed errors: `config set debug true`

### Build Errors

If you encounter build errors:

1. Ensure you have Go 1.18 or higher: `go version`
2. Verify dependencies are installed: `go mod tidy`
3. Check for the correct Aerospike client version: `go get github.com/aerospike/aerospike-client-go/v7`
4. Ensure readline library is installed: `go get github.com/chzyer/readline`

## Advanced Usage

### UDF Execution Patterns

The unified `execute-udf` command supports four execution patterns:

**1. Single Record Execution (Synchronous with Result)**
```bash
aerospike> execute-udf myudf.getValue(field) ON test.users WHERE PK = user123
```
- Returns the UDF result immediately
- Best for single record operations where you need the return value

**2. Query Execution (Background)**
```bash
aerospike> execute-udf myudf.update(status,active) ON test.users WHERE age = 30
```
- Runs as a background job on the server
- Executes on all records matching the query filter
- Requires a secondary index on the queried bin

**3. Range Query Execution (Background)**
```bash
aerospike> execute-udf myudf.adjust(discount,0.1) ON test.users WHERE age BETWEEN 18 AND 65
```
- Runs as a background job
- Executes on records within the specified range
- Requires a secondary index on the range bin

**4. Scan Execution (Background)**
```bash
aerospike> execute-udf myudf.migrate(version,2) ON test.users
```
- Runs as a background job
- Executes on ALL records in the namespace/set
- No index required

**Use cases for background UDFs:**
- Bulk updates across many records
- Data migration or transformation
- Applying business rules to filtered datasets
- Setting default values on existing records

### Multiple Bins in Batch Write

```bash
aerospike> batch-put key1:bin1=val1,bin2=val2,bin3=val3 key2:bin1=valA,bin2=valB
```

### Quoted Values

Use quotes for values with spaces:
```bash
aerospike> put user1 name="John Doe" address="123 Main St"
```

### Complex Queries

After creating appropriate indexes, you can query various data types:

```bash
# Numeric query
aerospike> create-index idx_age age numeric
aerospike> query age 30

# String query
aerospike> create-index idx_status status string
aerospike> query status active
```

## Performance Tips

1. Use batch operations for multiple records instead of individual puts/gets
2. Create secondary indexes on frequently queried bins
3. Adjust timeout policies based on your network latency
4. Use the scan command sparingly on large datasets
5. Use `hotget` and `hotput` to identify performance bottlenecks
6. Enable `send-key` policy only when necessary (increases storage)
7. Use `durable-delete` for critical deletions that must be persisted

## Policy Best Practices

### Write Policies

- **Use CREATE_ONLY** for insert-only operations to prevent accidental updates
- **Enable durable-delete** for compliance or audit requirements
- **Use generation-policy** for optimistic concurrency control (prevent lost updates)
- **Set appropriate TTL** to manage data lifecycle automatically
- **Enable send-key** when you need to retrieve keys during scans

### Read Policies

- **Use MASTER replica** for strong consistency requirements
- **Use MASTER_PROLES** for load distribution with eventual consistency
- **Use SESSION read-mode-sc** for session consistency in SC namespaces
- **Use LINEARIZE** when you need strongest consistency guarantees

### Commit Levels

- **COMMIT_ALL** (default) - Wait for all replicas, ensures durability
- **COMMIT_MASTER** - Faster writes, lower durability guarantee

## License

This tool is provided as-is for use with Aerospike databases.

## Support

For issues related to:
- **Aerospike Server**: Visit [Aerospike Documentation](https://docs.aerospike.com/)
- **Aerospike Go Client**: Visit [Aerospike Go Client GitHub](https://github.com/aerospike/aerospike-client-go)
- **This CLI Tool**: Check command syntax with `help` command

## Contributing

Feel free to extend this tool with additional features such as:
- Range queries
- CDT (Collection Data Type) operations
- More complex UDF examples
- Statistics and monitoring commands
- Export/import functionality
- Scan with UDF execution
- Background query operations
