# Aerospike Interactive CLI Client

A powerful command-line interface for interacting with Aerospike databases. This tool provides an interactive prompt with command history, full CRUD operations, secondary index management, and batch operations.

## Features

- **Interactive Prompt** with command history (up/down arrow navigation)
- **CRUD Operations** - Create, Read, Update, Delete records
- **Secondary Index Management** - Create and drop indexes
- **Query Support** - Query by secondary indexes
- **Batch Operations** - Batch reads and writes
- **Configurable Policies** - Socket timeout, total timeout, max retries, connect timeout
- **Debug Mode** - Detailed error codes and messages
- **Dynamic Configuration** - Change namespace/set on the fly

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

#### Delete a Record

```bash
aerospike> delete user1
```

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

### Configuration Commands

#### View Current Configuration

```bash
aerospike> config show
```

#### Change Configuration at Runtime

Enable debug mode:
```bash
aerospike> config set debug true
```

Change socket timeout:
```bash
aerospike> config set socket-timeout 5000
```

Change max retries:
```bash
aerospike> config set max-retries 5
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
aerospike> put user1 name=Alice age=25 city=NYC
aerospike> put user2 name=Bob age=30 city=LA
aerospike> put user3 name=Charlie age=25 city=SF

# Query by age
aerospike> query age 25

# Batch read
aerospike> batch-get user1 user2 user3

# Update a record
aerospike> put user1 name=Alice age=26 city=NYC

# Delete a record
aerospike> delete user3

# Scan all records
aerospike> scan

# Drop the index
aerospike> drop-index idx_age

# Exit
aerospike> exit
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

### Timeout Errors

If you encounter timeout errors:

1. Increase socket timeout: `config set socket-timeout 60000`
2. Increase total timeout: `config set total-timeout 60000`
3. Check network connectivity and server load

### Build Errors

If you encounter build errors:

1. Ensure you have Go 1.18 or higher: `go version`
2. Verify dependencies are installed: `go mod tidy`
3. Check for the correct Aerospike client version: `go get github.com/aerospike/aerospike-client-go/v7`

## Advanced Usage

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
- UDF (User Defined Function) execution
- Statistics and monitoring commands
- Export/import functionality
