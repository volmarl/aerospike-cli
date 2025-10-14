package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/chzyer/readline"
)

type Config struct {
	Host           string
	Port           int
	Namespace      string
	Set            string
	Debug          bool
	SocketTimeout  time.Duration
	TotalTimeout   time.Duration
	MaxRetries     int
	ConnectTimeout time.Duration
}

type CLI struct {
	client *aero.Client
	config *Config
}

func main() {
	config := parseArgs()
	
	cli, err := newCLI(config)
	if err != nil {
		fmt.Printf("Error connecting to Aerospike: %v\n", err)
		os.Exit(1)
	}
	defer cli.client.Close()

	fmt.Println("Aerospike Interactive Client")
	fmt.Println("Type 'help' for available commands")
	fmt.Println("=====================================")

	cli.run()
}

func parseArgs() *Config {
	config := &Config{
		Host:           "localhost",
		Port:           3000,
		Namespace:      "test",
		Set:            "",
		Debug:          false,
		SocketTimeout:  30 * time.Second,
		TotalTimeout:   0,
		MaxRetries:     2,
		ConnectTimeout: 1 * time.Second,
	}

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--host":
			if i+1 < len(args) {
				config.Host = args[i+1]
				i++
			}
		case "-p", "--port":
			if i+1 < len(args) {
				port, _ := strconv.Atoi(args[i+1])
				config.Port = port
				i++
			}
		case "-n", "--namespace":
			if i+1 < len(args) {
				config.Namespace = args[i+1]
				i++
			}
		case "-s", "--set":
			if i+1 < len(args) {
				config.Set = args[i+1]
				i++
			}
		case "-d", "--debug":
			config.Debug = true
		case "--socket-timeout":
			if i+1 < len(args) {
				timeout, _ := strconv.Atoi(args[i+1])
				config.SocketTimeout = time.Duration(timeout) * time.Millisecond
				i++
			}
		case "--total-timeout":
			if i+1 < len(args) {
				timeout, _ := strconv.Atoi(args[i+1])
				config.TotalTimeout = time.Duration(timeout) * time.Millisecond
				i++
			}
		case "--max-retries":
			if i+1 < len(args) {
				retries, _ := strconv.Atoi(args[i+1])
				config.MaxRetries = retries
				i++
			}
		case "--connect-timeout":
			if i+1 < len(args) {
				timeout, _ := strconv.Atoi(args[i+1])
				config.ConnectTimeout = time.Duration(timeout) * time.Millisecond
				i++
			}
		case "--help":
			printUsage()
			os.Exit(0)
		}
	}

	return config
}

func printUsage() {
	fmt.Println("Usage: aerospike-cli [options]")
	fmt.Println("\nConnection Options:")
	fmt.Println("  -h, --host <host>              Aerospike host (default: localhost)")
	fmt.Println("  -p, --port <port>              Aerospike port (default: 3000)")
	fmt.Println("  -n, --namespace <namespace>    Default namespace (default: test)")
	fmt.Println("  -s, --set <set>                Default set (optional)")
	fmt.Println("  -d, --debug                    Enable debug mode")
	fmt.Println("\nPolicy Options:")
	fmt.Println("  --socket-timeout <ms>          Socket timeout in milliseconds (default: 30000)")
	fmt.Println("  --total-timeout <ms>           Total timeout in milliseconds (default: 0)")
	fmt.Println("  --max-retries <n>              Maximum retries (default: 2)")
	fmt.Println("  --connect-timeout <ms>         Connect timeout in milliseconds (default: 1000)")
}

func newCLI(config *Config) (*CLI, error) {
	clientPolicy := aero.NewClientPolicy()
	clientPolicy.Timeout = config.ConnectTimeout

	client, err := aero.NewClient(config.Host, config.Port)
	if err != nil {
		return nil, err
	}

	return &CLI{
		client: client,
		config: config,
	}, nil
}

func (c *CLI) run() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[32maerospike>\033[0m ",
		HistoryFile:     "/tmp/.aerospike_history",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		return
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if line == "exit" || line == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		c.handleCommand(line)
	}
}

func (c *CLI) handleCommand(line string) {
	parts := parseCommand(line)
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "help":
		c.printHelp()
	case "put":
		c.handlePut(parts[1:])
	case "get":
		c.handleGet(parts[1:])
	case "delete":
		c.handleDelete(parts[1:])
	case "query":
		c.handleQuery(parts[1:])
	case "scan":
		c.handleScan(parts[1:])
	case "create-index":
		c.handleCreateIndex(parts[1:])
	case "drop-index":
		c.handleDropIndex(parts[1:])
	case "batch-put":
		c.handleBatchPut(parts[1:])
	case "batch-get":
		c.handleBatchGet(parts[1:])
	case "config":
		c.handleConfig(parts[1:])
	case "use":
		c.handleUse(parts[1:])
	default:
		fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", cmd)
	}
}

func (c *CLI) printHelp() {
	fmt.Println("\nAvailable Commands:")
	fmt.Println("  put <key> <bin1>=<val1> [bin2=val2 ...]  - Insert/update a record")
	fmt.Println("  get <key>                                  - Get a record by primary key")
	fmt.Println("  delete <key>                               - Delete a record")
	fmt.Println("  query <bin> <value>                        - Query by secondary index")
	fmt.Println("  scan                                       - Scan all records in set")
	fmt.Println("  create-index <name> <bin> <type>           - Create secondary index (numeric|string)")
	fmt.Println("  drop-index <name>                          - Drop secondary index")
	fmt.Println("  batch-put <key1:bin=val> <key2:bin=val>   - Batch write records")
	fmt.Println("  batch-get <key1> <key2> ...                - Batch read records")
	fmt.Println("  config [show|set <param> <value>]          - View/modify configuration")
	fmt.Println("  use <namespace> [set]                      - Change namespace/set")
	fmt.Println("  help                                       - Show this help")
	fmt.Println("  exit, quit                                 - Exit the client")
}

func (c *CLI) handlePut(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: put <key> <bin1>=<val1> [bin2=val2 ...]")
		return
	}

	key, err := aero.NewKey(c.config.Namespace, c.config.Set, args[0])
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	bins := make(aero.BinMap)
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			fmt.Printf("Invalid bin format: %s (use bin=value)\n", arg)
			continue
		}
		bins[parts[0]] = c.parseValue(parts[1])
	}

	policy := c.getWritePolicy()
	err = c.client.Put(policy, key, bins)
	if err != nil {
		c.logError("Put failed", err)
		return
	}

	fmt.Println("Record written successfully")
}

func (c *CLI) handleGet(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: get <key>")
		return
	}

	key, err := aero.NewKey(c.config.Namespace, c.config.Set, args[0])
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	policy := c.getReadPolicy()
	record, err := c.client.Get(policy, key)
	if err != nil {
		c.logError("Get failed", err)
		return
	}

	if record == nil {
		fmt.Println("Record not found")
		return
	}

	c.printRecord(key, record)
}

func (c *CLI) handleDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: delete <key>")
		return
	}

	key, err := aero.NewKey(c.config.Namespace, c.config.Set, args[0])
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	policy := c.getWritePolicy()
	existed, err := c.client.Delete(policy, key)
	if err != nil {
		c.logError("Delete failed", err)
		return
	}

	if existed {
		fmt.Println("Record deleted successfully")
	} else {
		fmt.Println("Record did not exist")
	}
}

func (c *CLI) handleQuery(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: query <bin> <value>")
		return
	}

	binName := args[0]
	value := c.parseValue(args[1])

	stmt := aero.NewStatement(c.config.Namespace, c.config.Set)
	stmt.SetFilter(aero.NewEqualFilter(binName, value))

	policy := c.getQueryPolicy()
	recordset, err := c.client.Query(policy, stmt)
	if err != nil {
		c.logError("Query failed", err)
		return
	}
	defer recordset.Close()

	count := 0
	fmt.Printf("\nQuerying where %s = %v\n", binName, value)
	fmt.Println("----------------------------------------")

	for record := range recordset.Results() {
		if record.Err != nil {
			c.logError("Query error", record.Err)
			continue
		}
		count++
		c.printRecord(record.Record.Key, record.Record)
	}

	fmt.Printf("\nTotal records: %d\n", count)
}

func (c *CLI) handleScan(args []string) {
	policy := c.getScanPolicy()
	recordset, err := c.client.ScanAll(policy, c.config.Namespace, c.config.Set)
	if err != nil {
		c.logError("Scan failed", err)
		return
	}
	defer recordset.Close()

	count := 0
	fmt.Printf("\nScanning namespace '%s', set '%s'\n", c.config.Namespace, c.config.Set)
	fmt.Println("----------------------------------------")

	for record := range recordset.Results() {
		if record.Err != nil {
			c.logError("Scan error", record.Err)
			continue
		}
		count++
		c.printRecord(record.Record.Key, record.Record)
	}

	fmt.Printf("\nTotal records: %d\n", count)
}

func (c *CLI) handleCreateIndex(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: create-index <name> <bin> <type>")
		fmt.Println("Types: numeric, string")
		return
	}

	indexName := args[0]
	binName := args[1]
	indexType := strings.ToLower(args[2])

	var idxType aero.IndexType
	switch indexType {
	case "numeric", "int", "integer":
		idxType = aero.NUMERIC
	case "string", "str":
		idxType = aero.STRING
	default:
		fmt.Printf("Invalid index type: %s (use 'numeric' or 'string')\n", indexType)
		return
	}

	task, err := c.client.CreateIndex(
		nil,
		c.config.Namespace,
		c.config.Set,
		indexName,
		binName,
		idxType,
	)
	if err != nil {
		c.logError("Create index failed", err)
		return
	}

	// Wait for index creation to complete
	err = <-task.OnComplete()
	if err != nil {
		c.logError("Index creation task failed", err)
		return
	}

	fmt.Printf("Secondary index '%s' created successfully on bin '%s' (%s)\n", indexName, binName, indexType)
}

func (c *CLI) handleDropIndex(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: drop-index <name>")
		return
	}

	indexName := args[0]

	err := c.client.DropIndex(
		nil,
		c.config.Namespace,
		c.config.Set,
		indexName,
	)
	if err != nil {
		c.logError("Drop index failed", err)
		return
	}

	fmt.Printf("Secondary index '%s' dropped successfully\n", indexName)
}

func (c *CLI) handleBatchPut(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: batch-put <key1:bin=val> <key2:bin=val> ...")
		return
	}

	var keys []*aero.Key
	var bins []aero.BinMap

	for _, arg := range args {
		parts := strings.SplitN(arg, ":", 2)
		if len(parts) != 2 {
			fmt.Printf("Invalid format: %s (use key:bin=value)\n", arg)
			continue
		}

		key, err := aero.NewKey(c.config.Namespace, c.config.Set, parts[0])
		if err != nil {
			c.logError("Error creating key", err)
			continue
		}

		binMap := make(aero.BinMap)
		binParts := strings.Split(parts[1], ",")
		for _, binPart := range binParts {
			kv := strings.SplitN(binPart, "=", 2)
			if len(kv) != 2 {
				fmt.Printf("Invalid bin format: %s\n", binPart)
				continue
			}
			binMap[kv[0]] = c.parseValue(kv[1])
		}

		keys = append(keys, key)
		bins = append(bins, binMap)
	}

	if len(keys) == 0 {
		fmt.Println("No valid records to write")
		return
	}

	writePolicy := c.getWritePolicy()
	for i, key := range keys {
		err := c.client.Put(writePolicy, key, bins[i])
		if err != nil {
			c.logError(fmt.Sprintf("Batch put failed for key %v", key.Value()), err)
		}
	}

	fmt.Printf("Batch write completed: %d records\n", len(keys))
}

func (c *CLI) handleBatchGet(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: batch-get <key1> <key2> ...")
		return
	}

	var keys []*aero.Key
	for _, arg := range args {
		key, err := aero.NewKey(c.config.Namespace, c.config.Set, arg)
		if err != nil {
			c.logError("Error creating key", err)
			continue
		}
		keys = append(keys, key)
	}

	if len(keys) == 0 {
		fmt.Println("No valid keys provided")
		return
	}

	policy := c.getBatchPolicy()
	records, err := c.client.BatchGet(policy, keys)
	if err != nil {
		c.logError("Batch get failed", err)
		return
	}

	fmt.Printf("\nBatch read: %d keys\n", len(keys))
	fmt.Println("----------------------------------------")

	for i, record := range records {
		if record != nil {
			c.printRecord(keys[i], record)
		} else {
			fmt.Printf("Key: %v - NOT FOUND\n", keys[i].Value())
		}
	}
}

func (c *CLI) handleConfig(args []string) {
	if len(args) == 0 || args[0] == "show" {
		fmt.Println("\nCurrent Configuration:")
		fmt.Printf("  Host:            %s\n", c.config.Host)
		fmt.Printf("  Port:            %d\n", c.config.Port)
		fmt.Printf("  Namespace:       %s\n", c.config.Namespace)
		fmt.Printf("  Set:             %s\n", c.config.Set)
		fmt.Printf("  Debug:           %v\n", c.config.Debug)
		fmt.Printf("  SocketTimeout:   %v\n", c.config.SocketTimeout)
		fmt.Printf("  TotalTimeout:    %v\n", c.config.TotalTimeout)
		fmt.Printf("  MaxRetries:      %d\n", c.config.MaxRetries)
		fmt.Printf("  ConnectTimeout:  %v\n", c.config.ConnectTimeout)
		return
	}

	if args[0] == "set" && len(args) >= 3 {
		param := args[1]
		value := args[2]

		switch param {
		case "debug":
			c.config.Debug = value == "true"
			fmt.Printf("Debug set to: %v\n", c.config.Debug)
		case "socket-timeout":
			timeout, _ := strconv.Atoi(value)
			c.config.SocketTimeout = time.Duration(timeout) * time.Millisecond
			fmt.Printf("SocketTimeout set to: %v\n", c.config.SocketTimeout)
		case "total-timeout":
			timeout, _ := strconv.Atoi(value)
			c.config.TotalTimeout = time.Duration(timeout) * time.Millisecond
			fmt.Printf("TotalTimeout set to: %v\n", c.config.TotalTimeout)
		case "max-retries":
			retries, _ := strconv.Atoi(value)
			c.config.MaxRetries = retries
			fmt.Printf("MaxRetries set to: %d\n", c.config.MaxRetries)
		default:
			fmt.Printf("Unknown parameter: %s\n", param)
		}
	} else {
		fmt.Println("Usage: config [show|set <param> <value>]")
	}
}

func (c *CLI) handleUse(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: use <namespace> [set]")
		return
	}

	c.config.Namespace = args[0]
	if len(args) > 1 {
		c.config.Set = args[1]
	} else {
		c.config.Set = ""
	}

	fmt.Printf("Using namespace: %s", c.config.Namespace)
	if c.config.Set != "" {
		fmt.Printf(", set: %s", c.config.Set)
	}
	fmt.Println()
}

func (c *CLI) getWritePolicy() *aero.WritePolicy {
	policy := aero.NewWritePolicy(0, 0)
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	return policy
}

func (c *CLI) getReadPolicy() *aero.BasePolicy {
	policy := aero.NewPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	return policy
}

func (c *CLI) getQueryPolicy() *aero.QueryPolicy {
	policy := aero.NewQueryPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	return policy
}

func (c *CLI) getScanPolicy() *aero.ScanPolicy {
	policy := aero.NewScanPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	return policy
}

func (c *CLI) getBatchPolicy() *aero.BatchPolicy {
	policy := aero.NewBatchPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	return policy
}

func (c *CLI) parseValue(s string) interface{} {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return s
}

func (c *CLI) printRecord(key *aero.Key, record *aero.Record) {
	fmt.Printf("\nKey: %v\n", key.Value())
	fmt.Printf("Generation: %d, Expiration: %d\n", record.Generation, record.Expiration)
	fmt.Println("Bins:")
	for name, value := range record.Bins {
		fmt.Printf("  %s: %v (%T)\n", name, value, value)
	}
}

func (c *CLI) logError(msg string, err error) {
	if c.config.Debug {
		if aerr, ok := err.(*aero.AerospikeError); ok {
			fmt.Printf("ERROR: %s\n", msg)
			fmt.Printf("  ResultCode: %v\n", aerr.ResultCode)
			fmt.Printf("  Message: %s\n", aerr.Error())
			if aerr.InDoubt {
				fmt.Printf("  InDoubt: %v\n", aerr.InDoubt)
			}
		} else {
			fmt.Printf("ERROR: %s - %v\n", msg, err)
		}
	} else {
		fmt.Printf("ERROR: %s - %v\n", msg, err)
	}
}

func parseCommand(line string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false

	for _, r := range line {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case ' ':
			if inQuotes {
				current.WriteRune(r)
			} else if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}
