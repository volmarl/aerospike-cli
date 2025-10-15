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
	case "delete-digest":
		c.handleDeleteDigest(parts[1:])
	case "query":
		c.handleQuery(parts[1:])
	case "scan":
		c.handleScan(parts[1:])
	case "scan-touch":
		c.handleScanTouch(parts[1:])
	case "create-index":
		c.handleCreateIndex(parts[1:])
	case "drop-index":
		c.handleDropIndex(parts[1:])
	case "register-udf":
		c.handleRegisterUDF(parts[1:])
	case "list-udfs":
		c.handleListUDFs(parts[1:])
	case "remove-udf":
		c.handleRemoveUDF(parts[1:])
	case "execute-udf":
		c.handleExecuteUDF(parts[1:])
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
	fmt.Println("\n==============================================")
	fmt.Println("         Aerospike CLI - Commands")
	fmt.Println("==============================================")
	
	fmt.Println("\nBasic Operations:")
	fmt.Println("  put <key> <bin1>=<val1> [bin2=val2 ...]")
	fmt.Println("      Insert or update a record")
	fmt.Println("  get <key>")
	fmt.Println("      Get a record by primary key")
	fmt.Println("  delete <key>")
	fmt.Println("      Delete a record by primary key")
	fmt.Println("  delete-digest <digest>")
	fmt.Println("      Delete a record by its 20-byte digest (hex string)")
	fmt.Println("  scan")
	fmt.Println("      Scan all records in current namespace/set")
	fmt.Println("  scan-touch")
	fmt.Println("      Scan and touch all records (reset TTL to namespace default)")
	
	fmt.Println("\nSecondary Index Operations:")
	fmt.Println("  create-index <name> <bin> <type>")
	fmt.Println("      Create secondary index (type: numeric or string)")
	fmt.Println("  drop-index <name>")
	fmt.Println("      Drop secondary index")
	fmt.Println("  query <bin> <value>")
	fmt.Println("      Query records by secondary index")
	
	fmt.Println("\nUDF (User Defined Functions):")
	fmt.Println("  register-udf <path> [server_path]")
	fmt.Println("      Register UDF module from file")
	fmt.Println("  list-udfs")
	fmt.Println("      List all registered UDF modules")
	fmt.Println("  remove-udf <filename>")
	fmt.Println("      Remove UDF module from server")
	fmt.Println("  execute-udf <module>.<function>(<args>) ON <ns>[.<set>] [WHERE ...]")
	fmt.Println("      Execute UDF with various filters:")
	fmt.Println("        ON <ns>[.<set>]")
	fmt.Println("            Execute on all records (background)")
	fmt.Println("        WHERE PK = <key>")
	fmt.Println("            Execute on single record (returns result)")
	fmt.Println("        WHERE <bin> = <value>")
	fmt.Println("            Execute on query results (background)")
	fmt.Println("        WHERE <bin> BETWEEN <lower> AND <upper>")
	fmt.Println("            Execute on range query (background)")
	fmt.Println("      Examples:")
	fmt.Println("        execute-udf myudf.increment(salary,1000) ON test.users WHERE PK = user1")
	fmt.Println("        execute-udf myudf.process(status,active) ON test.users WHERE age = 30")
	fmt.Println("        execute-udf myudf.adjust(bonus,500) ON test WHERE age BETWEEN 25 AND 35")
	
	fmt.Println("\nBatch Operations:")
	fmt.Println("  batch-put <key1:bin=val> <key2:bin=val> ...")
	fmt.Println("      Batch write multiple records")
	fmt.Println("  batch-get <key1> <key2> ...")
	fmt.Println("      Batch read multiple records")
	
	fmt.Println("\nConfiguration:")
	fmt.Println("  config [show|set <param> <value>]")
	fmt.Println("      View or modify runtime configuration")
	fmt.Println("  use <namespace> [set]")
	fmt.Println("      Change current namespace/set")
	
	fmt.Println("\nGeneral:")
	fmt.Println("  help")
	fmt.Println("      Show this help message")
	fmt.Println("  exit, quit")
	fmt.Println("      Exit the client")
	fmt.Println("\n==============================================")
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

	c.printRecordWithMetadata(key, record)
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

func (c *CLI) handleDeleteDigest(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: delete-digest <digest>")
		fmt.Println("Example: delete-digest 0a1b2c3d4e5f6789abcdef0123456789abcdef01")
		fmt.Println("\nThe digest must be a 40-character hexadecimal string (20 bytes)")
		return
	}

	digestHex := args[0]
	
	// Remove any spaces or separators
	digestHex = strings.ReplaceAll(digestHex, " ", "")
	digestHex = strings.ReplaceAll(digestHex, ":", "")
	digestHex = strings.ReplaceAll(digestHex, "-", "")
	
	// Validate hex string length (should be 40 chars for 20 bytes)
	if len(digestHex) != 40 {
		fmt.Printf("Error: Digest must be exactly 40 hexadecimal characters (got %d)\n", len(digestHex))
		return
	}
	
	// Convert hex string to bytes
	digest := make([]byte, 20)
	for i := 0; i < 20; i++ {
		byteStr := digestHex[i*2 : i*2+2]
		b, err := strconv.ParseUint(byteStr, 16, 8)
		if err != nil {
			fmt.Printf("Error: Invalid hexadecimal character in digest: %s\n", byteStr)
			return
		}
		digest[i] = byte(b)
	}
	
	// Create key from digest
	key, err := aero.NewKeyWithDigest(c.config.Namespace, c.config.Set, nil, digest)
	if err != nil {
		c.logError("Error creating key from digest", err)
		return
	}

	policy := c.getWritePolicy()
	existed, err := c.client.Delete(policy, key)
	if err != nil {
		c.logError("Delete failed", err)
		return
	}

	if existed {
		fmt.Printf("Record with digest %s deleted successfully\n", digestHex)
	} else {
		fmt.Printf("Record with digest %s did not exist\n", digestHex)
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

func (c *CLI) handleScanTouch(args []string) {
	// Confirm the operation
	fmt.Printf("\nThis will touch ALL records in namespace '%s'", c.config.Namespace)
	if c.config.Set != "" {
		fmt.Printf(", set '%s'", c.config.Set)
	}
	fmt.Println()
	fmt.Println("Each record's TTL will be reset to the namespace default (TTL=-2)")
	fmt.Print("Are you sure you want to continue? (yes/no): ")
	
	var response string
	fmt.Scanln(&response)
	if strings.ToLower(response) != "yes" {
		fmt.Println("Operation cancelled")
		return
	}

	// Create a scan policy
	scanPolicy := c.getScanPolicy()
	
	// Create write policy for touch operation with TTL=-2
	writePolicy := c.getWritePolicy()
	writePolicy.Expiration = aero.TTLServerDefault // Use -2 to reset to namespace default
	writePolicy.RecordExistsAction = aero.UPDATE

	fmt.Printf("\nStarting scan-touch operation...\n")
	fmt.Println("----------------------------------------")
	
	recordset, err := c.client.ScanAll(scanPolicy, c.config.Namespace, c.config.Set)
	if err != nil {
		c.logError("Scan failed", err)
		return
	}
	defer recordset.Close()

	count := 0
	errors := 0
	
	for result := range recordset.Results() {
		if result.Err != nil {
			c.logError("Scan error", result.Err)
			errors++
			continue
		}
		
		// Touch the record (update with TTL=-2)
		err := c.client.Touch(writePolicy, result.Record.Key)
		if err != nil {
			if c.config.Debug {
				c.logError(fmt.Sprintf("Touch failed for key %v", result.Record.Key.Value()), err)
			}
			errors++
		} else {
			count++
			if count%1000 == 0 {
				fmt.Printf("Touched %d records...\n", count)
			}
		}
	}

	fmt.Println("\n----------------------------------------")
	fmt.Printf("Scan-touch completed\n")
	fmt.Printf("  Records touched: %d\n", count)
	if errors > 0 {
		fmt.Printf("  Errors: %d\n", errors)
	}
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

func (c *CLI) handleRegisterUDF(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: register-udf <path> [server_path]")
		fmt.Println("Example: register-udf /path/to/myudf.lua myudf.lua")
		return
	}

	clientPath := args[0]
	
	code, err := os.ReadFile(clientPath)
	if err != nil {
		c.logError("Failed to read UDF file", err)
		return
	}

	var serverPath string
	if len(args) > 1 {
		serverPath = args[1]
	} else {
		parts := strings.Split(clientPath, "/")
		serverPath = parts[len(parts)-1]
	}

	language := aero.LUA
	if strings.HasSuffix(serverPath, ".lua") {
		language = aero.LUA
	}

	task, err := c.client.RegisterUDF(nil, code, serverPath, language)
	if err != nil {
		c.logError("Failed to register UDF", err)
		return
	}

	err = <-task.OnComplete()
	if err != nil {
		c.logError("UDF registration task failed", err)
		return
	}

	fmt.Printf("UDF module '%s' registered successfully\n", serverPath)
}

func (c *CLI) handleListUDFs(args []string) {
	udfs, err := c.client.ListUDF(nil)
	if err != nil {
		c.logError("Failed to list UDFs", err)
		return
	}

	if len(udfs) == 0 {
		fmt.Println("No UDF modules registered")
		return
	}

	fmt.Println("\nRegistered UDF Modules:")
	fmt.Println("----------------------------------------")
	for _, udf := range udfs {
		fmt.Printf("Name: %s\n", udf.Filename)
		fmt.Printf("  Type: %s\n", udf.Language)
		fmt.Printf("  Hash: %s\n", udf.Hash)
		fmt.Println()
	}
	fmt.Printf("Total modules: %d\n", len(udfs))
}

func (c *CLI) handleRemoveUDF(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: remove-udf <filename>")
		return
	}

	filename := args[0]

	task, err := c.client.RemoveUDF(nil, filename)
	if err != nil {
		c.logError("Failed to remove UDF", err)
		return
	}

	err = <-task.OnComplete()
	if err != nil {
		c.logError("UDF removal task failed", err)
		return
	}

	fmt.Printf("UDF module '%s' removed successfully\n", filename)
}

func (c *CLI) handleExecuteUDF(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: execute-udf <module>.<function>(<args>) ON <ns>[.<set>] [WHERE ...]")
		fmt.Println("\nExamples:")
		fmt.Println("  execute-udf myudf.increment(salary,1000) ON test.users WHERE PK = user1")
		fmt.Println("  execute-udf myudf.process(status,active) ON test.users WHERE age = 30")
		fmt.Println("  execute-udf myudf.update(field,value) ON test.users WHERE age BETWEEN 25 AND 35")
		fmt.Println("  execute-udf myudf.process() ON test.users")
		return
	}

	cmdStr := strings.Join(args, " ")
	
	parts := strings.Split(cmdStr, " ON ")
	if len(parts) != 2 {
		fmt.Println("Error: Missing 'ON' clause")
		return
	}

	udfPart := strings.TrimSpace(parts[0])
	onPart := strings.TrimSpace(parts[1])

	if !strings.Contains(udfPart, ".") || !strings.Contains(udfPart, "(") {
		fmt.Println("Error: Invalid UDF format. Expected: <module>.<function>(<args>)")
		return
	}

	dotIdx := strings.Index(udfPart, ".")
	parenIdx := strings.Index(udfPart, "(")
	
	if dotIdx >= parenIdx {
		fmt.Println("Error: Invalid UDF format")
		return
	}

	moduleName := udfPart[:dotIdx]
	functionName := udfPart[dotIdx+1 : parenIdx]
	
	argsStr := udfPart[parenIdx+1:]
	if !strings.HasSuffix(argsStr, ")") {
		fmt.Println("Error: Missing closing parenthesis")
		return
	}
	argsStr = strings.TrimSuffix(argsStr, ")")
	
	var udfArgs []aero.Value
	if strings.TrimSpace(argsStr) != "" {
		argList := strings.Split(argsStr, ",")
		for _, arg := range argList {
			arg = strings.TrimSpace(arg)
			udfArgs = append(udfArgs, aero.NewValue(c.parseValue(arg)))
		}
	}

	whereParts := strings.Split(onPart, " WHERE ")
	nsPart := strings.TrimSpace(whereParts[0])
	
	var namespace, set string
	if strings.Contains(nsPart, ".") {
		nsSplit := strings.SplitN(nsPart, ".", 2)
		namespace = nsSplit[0]
		set = nsSplit[1]
	} else {
		namespace = nsPart
		set = ""
	}

	if len(whereParts) == 1 {
		c.executeUDFScan(namespace, set, moduleName, functionName, udfArgs)
		return
	}

	whereClause := strings.TrimSpace(whereParts[1])
	
	if strings.HasPrefix(strings.ToUpper(whereClause), "PK =") || 
	   strings.HasPrefix(strings.ToUpper(whereClause), "PK=") {
		keyValue := strings.TrimSpace(whereClause[strings.Index(whereClause, "=")+1:])
		c.executeUDFOnKey(namespace, set, keyValue, moduleName, functionName, udfArgs)
		return
	}

	if strings.Contains(strings.ToUpper(whereClause), " BETWEEN ") {
		c.executeUDFRange(namespace, set, whereClause, moduleName, functionName, udfArgs)
		return
	}

	if strings.Contains(whereClause, "=") {
		c.executeUDFQuery(namespace, set, whereClause, moduleName, functionName, udfArgs)
		return
	}

	fmt.Println("Error: Invalid WHERE clause")
}

func (c *CLI) executeUDFOnKey(namespace, set, keyValue, module, function string, args []aero.Value) {
	key, err := aero.NewKey(namespace, set, keyValue)
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	policy := c.getWritePolicy()
	result, err := c.client.Execute(policy, key, module, function, args...)
	if err != nil {
		c.logError("UDF execution failed", err)
		return
	}

	fmt.Println("\nUDF Execution Result:")
	fmt.Println("----------------------------------------")
	fmt.Printf("Key: %s\n", keyValue)
	if result != nil {
		fmt.Printf("Result: %v (%T)\n", result, result)
	} else {
		fmt.Println("Result: nil")
	}
}

func (c *CLI) executeUDFQuery(namespace, set, whereClause, module, function string, args []aero.Value) {
	parts := strings.SplitN(whereClause, "=", 2)
	if len(parts) != 2 {
		fmt.Println("Error: Invalid WHERE clause format")
		return
	}

	binName := strings.TrimSpace(parts[0])
	value := c.parseValue(strings.TrimSpace(parts[1]))

	stmt := aero.NewStatement(namespace, set)
	stmt.SetFilter(aero.NewEqualFilter(binName, value))

	policy := c.getQueryPolicy()
	
	fmt.Printf("\nExecuting UDF '%s.%s' on query results where %s = %v\n", module, function, binName, value)
	fmt.Println("----------------------------------------")

	task, err := c.client.ExecuteUDF(policy, stmt, module, function, args...)
	if err != nil {
		c.logError("Background UDF execution failed", err)
		return
	}

	fmt.Println("Background job initiated")
	fmt.Print("Waiting for completion")

	for {
		done, err := task.IsDone()
		if err != nil {
			c.logError("Error checking task status", err)
			return
		}
		if done {
			break
		}
		fmt.Print(".")
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n\nUDF execution completed successfully")
}

func (c *CLI) executeUDFRange(namespace, set, whereClause, module, function string, args []aero.Value) {
	upperClause := strings.ToUpper(whereClause)
	betweenIdx := strings.Index(upperClause, " BETWEEN ")
	andIdx := strings.LastIndex(upperClause, " AND ")
	
	if betweenIdx == -1 || andIdx == -1 || andIdx <= betweenIdx {
		fmt.Println("Error: Invalid BETWEEN clause format")
		return
	}

	binName := strings.TrimSpace(whereClause[:betweenIdx])
	lowerStr := strings.TrimSpace(whereClause[betweenIdx+9 : andIdx])
	upperStr := strings.TrimSpace(whereClause[andIdx+5:])

	lower := c.parseValue(lowerStr)
	upper := c.parseValue(upperStr)

	var lowerInt, upperInt int64
	switch v := lower.(type) {
	case int64:
		lowerInt = v
	case int:
		lowerInt = int64(v)
	case float64:
		lowerInt = int64(v)
	default:
		fmt.Printf("Error: Lower bound must be numeric, got %T\n", lower)
		return
	}

	switch v := upper.(type) {
	case int64:
		upperInt = v
	case int:
		upperInt = int64(v)
	case float64:
		upperInt = int64(v)
	default:
		fmt.Printf("Error: Upper bound must be numeric, got %T\n", upper)
		return
	}

	stmt := aero.NewStatement(namespace, set)
	stmt.SetFilter(aero.NewRangeFilter(binName, lowerInt, upperInt))

	policy := c.getQueryPolicy()
	
	fmt.Printf("\nExecuting UDF '%s.%s' on range query where %s BETWEEN %v AND %v\n", 
		module, function, binName, lowerInt, upperInt)
	fmt.Println("----------------------------------------")

	task, err := c.client.ExecuteUDF(policy, stmt, module, function, args...)
	if err != nil {
		c.logError("Background UDF execution failed", err)
		return
	}

	fmt.Println("Background job initiated")
	fmt.Print("Waiting for completion")

	for {
		done, err := task.IsDone()
		if err != nil {
			c.logError("Error checking task status", err)
			return
		}
		if done {
			break
		}
		fmt.Print(".")
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n\nUDF execution completed successfully")
}

func (c *CLI) executeUDFScan(namespace, set, module, function string, args []aero.Value) {
	stmt := aero.NewStatement(namespace, set)

	policy := c.getQueryPolicy()
	
	fmt.Printf("\nExecuting background UDF '%s.%s' on namespace='%s'", module, function, namespace)
	if set != "" {
		fmt.Printf(", set='%s'", set)
	}
	fmt.Println()
	fmt.Println("----------------------------------------")

	task, err := c.client.ExecuteUDF(policy, stmt, module, function, args...)
	if err != nil {
		c.logError("Background UDF execution failed", err)
		return
	}

	fmt.Println("Background job initiated")
	fmt.Print("Waiting for completion")

	for {
		done, err := task.IsDone()
		if err != nil {
			c.logError("Error checking task status", err)
			return
		}
		if done {
			break
		}
		fmt.Print(".")
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n\nUDF execution completed successfully")
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
	fmt.Printf("Digest: %x\n", key.Digest())
	fmt.Printf("Generation: %d, Expiration: %d\n", record.Generation, record.Expiration)
	fmt.Println("Bins:")
	for name, value := range record.Bins {
		fmt.Printf("  %s: %v (%T)\n", name, value, value)
	}
}

func (c *CLI) printRecordWithMetadata(key *aero.Key, record *aero.Record) {
	fmt.Printf("\nKey: %v\n", key.Value())
	fmt.Printf("Digest: %x\n", key.Digest())
	
	// Get partition information
	partition := key.PartitionId()
	fmt.Printf("Partition ID: %d\n", partition)
	
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
