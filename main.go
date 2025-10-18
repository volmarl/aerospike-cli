package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	
	// Write Policies
	RecordExistsAction string // UPDATE, UPDATE_ONLY, REPLACE, REPLACE_ONLY, CREATE_ONLY
	GenerationPolicy   string // NONE, EXPECT_GEN_EQUAL, EXPECT_GEN_GT
	Generation         uint32
	Expiration         int32  // TTL in seconds, -1=never expire, -2=namespace default, 0=use default
	DurableDelete      bool
	SendKey            bool   // Store user key on server
	
	// Read Policies
	ReadModeAP   string // ONE, ALL
	ReadModeSC   string // SESSION, LINEARIZE, ALLOW_REPLICA, ALLOW_UNAVAILABLE
	Replica      string // MASTER, MASTER_PROLES, RANDOM, SEQUENCE, PREFER_RACK
	
	// Commit Level
	CommitLevel string // COMMIT_ALL, COMMIT_MASTER
}

type CLI struct {
	client *aero.Client
	config *Config
}

// LoadTest statistics
type LoadTestStats struct {
	totalOps    int64
	successOps  int64
	errorOps    int64
	timeoutOps  int64
	startTime   time.Time
	latencies   []int64 // microseconds
	latencyLock sync.Mutex
}

func (s *LoadTestStats) recordLatency(latency int64) {
	s.latencyLock.Lock()
	defer s.latencyLock.Unlock()
	s.latencies = append(s.latencies, latency)
}

func (s *LoadTestStats) getPercentiles() (p50, p95, p99, pMax int64) {
	s.latencyLock.Lock()
	defer s.latencyLock.Unlock()
	
	if len(s.latencies) == 0 {
		return 0, 0, 0, 0
	}
	
	// Simple percentile calculation
	sorted := make([]int64, len(s.latencies))
	copy(sorted, s.latencies)
	
	// Bubble sort (fine for small samples, use quicksort for production)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	pMax = sorted[len(sorted)-1]
	
	return
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
		
		// Write Policy defaults
		RecordExistsAction: "UPDATE",
		GenerationPolicy:   "NONE",
		Generation:         0,
		Expiration:         0,
		DurableDelete:      false,
		SendKey:            false,
		
		// Read Policy defaults
		ReadModeAP:   "ONE",
		ReadModeSC:   "SESSION",
		Replica:      "SEQUENCE",
		
		// Commit Level default
		CommitLevel: "COMMIT_ALL",
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
	case "debug-record-meta":
		c.handleDebugRecordMeta(parts[1:])
	case "query":
		c.handleQuery(parts[1:])
	case "scan":
		c.handleScan(parts[1:])
	case "scan-touch":
		c.handleScanTouch(parts[1:])
	case "hotget":
		c.handleHotGet(parts[1:])
	case "hotput":
		c.handleHotPut(parts[1:])
	case "loadtest-read":
		c.handleLoadTestRead(parts[1:])
	case "loadtest-write":
		c.handleLoadTestWrite(parts[1:])
	case "loadtest-batch-read":
		c.handleLoadTestBatchRead(parts[1:])
	case "loadtest-batch-write":
		c.handleLoadTestBatchWrite(parts[1:])
	case "create-index":
		c.handleCreateIndex(parts[1:])
	case "drop-index":
		c.handleDropIndex(parts[1:])
	case "show-indexes", "show-sindex":
		c.handleShowIndexes(parts[1:])
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
	fmt.Println("  debug-record-meta <digest>")
	fmt.Println("      Show detailed record metadata (no bin data)")
	fmt.Println("  scan")
	fmt.Println("      Scan all records in current namespace/set")
	fmt.Println("  scan-touch")
	fmt.Println("      Scan and touch all records (reset TTL to namespace default)")
	
	fmt.Println("\nLoad Testing (Distributed Workloads):")
	fmt.Println("  loadtest-read [keyPrefix] [numKeys] [threads] [duration] [rate]")
	fmt.Println("      Distributed read workload across multiple keys")
	fmt.Println("      keyPrefix: prefix for keys (default: loadtest)")
	fmt.Println("      numKeys: number of unique keys (default: 1000)")
	fmt.Println("      threads: concurrent workers (default: 50)")
	fmt.Println("      duration: test duration in seconds (default: 60)")
	fmt.Println("      rate: target ops/sec per thread (default: 100, 0=unlimited)")
	fmt.Println("  loadtest-write [keyPrefix] [numKeys] [threads] [duration] [rate]")
	fmt.Println("      Distributed write workload across multiple keys")
	fmt.Println("  loadtest-batch-read [keyPrefix] [numKeys] [batchSize] [threads] [duration] [rate]")
	fmt.Println("      Batch read workload")
	fmt.Println("      batchSize: keys per batch request (default: 10)")
	fmt.Println("  loadtest-batch-write [keyPrefix] [numKeys] [batchSize] [threads] [duration] [rate]")
	fmt.Println("      Batch write workload")
	
	fmt.Println("\nHot Key Testing (Single Key Contention):")
	fmt.Println("  hotget <key> [connections] [duration] [rate]")
	fmt.Println("      Generate high-rate read workload on a single key")
	fmt.Println("      connections: number of concurrent connections (default: 100)")
	fmt.Println("      duration: test duration in seconds (default: 60)")
	fmt.Println("      rate: target ops/sec per connection (default: 1000, 0=unlimited)")
	fmt.Println("  hotput <key> [connections] [duration] [rate]")
	fmt.Println("      Generate high-rate write workload on a single key")
	
	fmt.Println("\nSecondary Index Operations:")
	fmt.Println("  create-index <name> <bin> <type>")
	fmt.Println("      Create secondary index (type: numeric or string)")
	fmt.Println("  show-indexes")
	fmt.Println("      List all secondary indexes with details")
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

func (c *CLI) handleDebugRecordMeta(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: debug-record-meta <digest>")
		fmt.Println("Example: debug-record-meta 0a1b2c3d4e5f6789abcdef0123456789abcdef01")
		fmt.Println("\nThe digest must be a 40-character hexadecimal string (20 bytes)")
		fmt.Println("Returns record metadata including XDR write flag, replication status, and tombstone flag")
		return
	}

	digestHex := args[0]
	
	// Remove any spaces or separators
	digestHex = strings.ReplaceAll(digestHex, " ", "")
	digestHex = strings.ReplaceAll(digestHex, ":", "")
	digestHex = strings.ReplaceAll(digestHex, "-", "")
	
	// Validate hex string length
	if len(digestHex) != 40 {
		fmt.Printf("Error: Digest must be exactly 40 hexadecimal characters (got %d)\n", len(digestHex))
		return
	}
	
	// Convert to uppercase for the info command
	digestHex = strings.ToUpper(digestHex)

	// Get a node to query
	nodes := c.client.GetNodes()
	if len(nodes) == 0 {
		fmt.Println("Error: No nodes available")
		return
	}
	node := nodes[0]

	// Build the info command for debug-record-meta
	// Format: debug-record-meta:namespace=<ns>;keyd=<digest_hex>;
	infoCmd := fmt.Sprintf("debug-record-meta:namespace=%s;keyd=%s;", c.config.Namespace, digestHex)
	if c.config.Set != "" {
		infoCmd = fmt.Sprintf("debug-record-meta:namespace=%s;set=%s;keyd=%s;", 
			c.config.Namespace, c.config.Set, digestHex)
	}

	// Execute info command with a policy
	policy := aero.NewInfoPolicy()
	infoMap, err := node.RequestInfo(policy, infoCmd)
	if err != nil {
		c.logError("Failed to retrieve record metadata", err)
		return
	}

	result, exists := infoMap[infoCmd]
	if !exists || result == "" {
		fmt.Println("No metadata returned from server")
		return
	}

	if strings.Contains(result, "ERROR") || strings.Contains(result, "NOT_FOUND") {
		fmt.Printf("Record with digest %s not found or error occurred\n", digestHex)
		if c.config.Debug {
			fmt.Printf("Server response: %s\n", result)
		}
		return
	}

	// Parse the result
	// Format: pid=<pid>;repl-ix=<idx>;index=rc=0,tree-id=1,...;key=...;n-bins=<n>;bins=...
	fmt.Println("\nRecord Metadata:")
	fmt.Println("========================================")
	fmt.Printf("Digest (keyd): %s\n", digestHex)
	fmt.Printf("Namespace: %s\n", c.config.Namespace)
	if c.config.Set != "" {
		fmt.Printf("Set: %s\n", c.config.Set)
	}
	fmt.Println("----------------------------------------")
	
	// Parse the response - split by semicolon for major sections
	sections := strings.Split(result, ";")
	
	metadata := make(map[string]string)
	indexMetadata := make(map[string]string)
	
	for _, section := range sections {
		section = strings.TrimSpace(section)
		if section == "" {
			continue
		}
		
		// Check if this is the index section
		if strings.HasPrefix(section, "index=") {
			// Parse index metadata
			indexData := strings.TrimPrefix(section, "index=")
			pairs := strings.Split(indexData, ",")
			for _, pair := range pairs {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					indexMetadata[kv[0]] = kv[1]
				}
			}
		} else if strings.HasPrefix(section, "key=") {
			metadata["key"] = strings.TrimPrefix(section, "key=")
		} else if strings.HasPrefix(section, "n-bins=") {
			metadata["n-bins"] = strings.TrimPrefix(section, "n-bins=")
		} else if strings.HasPrefix(section, "bins=") {
			metadata["bins"] = strings.TrimPrefix(section, "bins=")
		} else {
			// Simple key=value pair
			kv := strings.SplitN(section, "=", 2)
			if len(kv) == 2 {
				metadata[kv[0]] = kv[1]
			}
		}
	}
	
	// Display core metadata
	if gen, ok := indexMetadata["generation"]; ok {
		fmt.Printf("Generation: %s\n", gen)
	}
	if vt, ok := indexMetadata["void-time"]; ok {
		fmt.Printf("Void Time (Expiration): %s\n", vt)
	}
	if lut, ok := indexMetadata["lut"]; ok {
		fmt.Printf("Last Update Time: %s", lut)
		// Convert Aerospike epoch (citrusleaf epoch) to readable time
		// Aerospike epoch starts at 2010-01-01 00:00:00 UTC
		if lutVal, err := strconv.ParseInt(lut, 10, 64); err == nil {
			// Convert from milliseconds to seconds and add to Aerospike epoch
			aerospikeEpoch := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
			recordTime := aerospikeEpoch.Add(time.Duration(lutVal) * time.Millisecond)
			fmt.Printf(" (%s)\n", recordTime.Format("2006-01-02 15:04:05 MST"))
		} else {
			fmt.Println()
		}
	}
	if nBins, ok := metadata["n-bins"]; ok {
		fmt.Printf("Number of Bins: %s\n", nBins)
	}
	if setName, ok := indexMetadata["set-name"]; ok {
		fmt.Printf("Set Name: %s\n", setName)
	}
	
	// XDR and replication flags
	fmt.Println("----------------------------------------")
	fmt.Println("Flags:")
	if xdrWrite, ok := indexMetadata["xdr-write"]; ok {
		fmt.Printf("  XDR Write: %s\n", xdrWrite)
	}
	if xdrTombstone, ok := indexMetadata["xdr-tombstone"]; ok {
		fmt.Printf("  XDR Tombstone: %s\n", xdrTombstone)
	}
	if xdrNsupTombstone, ok := indexMetadata["xdr-nsup-tombstone"]; ok {
		fmt.Printf("  XDR NSUP Tombstone: %s\n", xdrNsupTombstone)
	}
	if tombstone, ok := indexMetadata["tombstone"]; ok {
		fmt.Printf("  Tombstone: %s\n", tombstone)
	}
	if cenotaph, ok := indexMetadata["cenotaph"]; ok {
		fmt.Printf("  Cenotaph: %s\n", cenotaph)
	}
	if replState, ok := indexMetadata["repl-state"]; ok {
		fmt.Printf("  Replication State: %s\n", replState)
	}
	if keyStored, ok := indexMetadata["key-stored"]; ok {
		fmt.Printf("  Key Stored: %s\n", keyStored)
	}
	
	// Index information
	fmt.Println("----------------------------------------")
	fmt.Println("Index Information:")
	if pid, ok := metadata["pid"]; ok {
		fmt.Printf("  Partition ID: %s\n", pid)
	}
	if replIx, ok := metadata["repl-ix"]; ok {
		fmt.Printf("  Replica Index: %s\n", replIx)
	}
	if treeId, ok := indexMetadata["tree-id"]; ok {
		fmt.Printf("  Tree ID: %s\n", treeId)
	}
	if rc, ok := indexMetadata["rc"]; ok {
		fmt.Printf("  Reference Count: %s\n", rc)
	}
	
	// Storage information
	if rblockId, ok := indexMetadata["rblock-id"]; ok && rblockId != "0" {
		fmt.Println("----------------------------------------")
		fmt.Println("Storage Information:")
		fmt.Printf("  RBlock ID: %s\n", rblockId)
		if nRblocks, ok := indexMetadata["n-rblocks"]; ok {
			fmt.Printf("  Number of RBlocks: %s\n", nRblocks)
		}
		if fileId, ok := indexMetadata["file-id"]; ok {
			fmt.Printf("  File ID: %s\n", fileId)
		}
	}
	
	fmt.Println("========================================")
	
	if c.config.Debug {
		fmt.Println("\nRaw Response:")
		fmt.Println(result)
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

func (c *CLI) handleHotGet(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: hotget <key> [connections] [duration] [rate]")
		fmt.Println("\nGenerate a high-rate read workload on a single key")
		fmt.Println("\nParameters:")
		fmt.Println("  key          - Key to read repeatedly")
		fmt.Println("  connections  - Number of concurrent connections (default: 100)")
		fmt.Println("  duration     - Test duration in seconds (default: 60)")
		fmt.Println("  rate         - Target ops/sec per connection (default: 1000, 0=unlimited)")
		fmt.Println("\nExample:")
		fmt.Println("  hotget user1 200 30 5000")
		fmt.Println("  (200 connections, 30 seconds, 5000 ops/sec per connection)")
		return
	}

	keyValue := args[0]
	
	// Parse optional parameters
	connections := 100
	duration := 60
	targetRate := 1000
	
	if len(args) > 1 {
		if c, err := strconv.Atoi(args[1]); err == nil {
			connections = c
		}
	}
	if len(args) > 2 {
		if d, err := strconv.Atoi(args[2]); err == nil {
			duration = d
		}
	}
	if len(args) > 3 {
		if r, err := strconv.Atoi(args[3]); err == nil {
			targetRate = r
		}
	}

	// Create the key
	key, err := aero.NewKey(c.config.Namespace, c.config.Set, keyValue)
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	// Verify key exists first
	policy := c.getReadPolicy()
	record, err := c.client.Get(policy, key)
	if err != nil {
		c.logError("Failed to read key (does it exist?)", err)
		return
	}
	if record == nil {
		fmt.Printf("Key '%s' not found. Please create it first.\n", keyValue)
		return
	}

	fmt.Println("\n========================================")
	fmt.Println("         Hot Key Read Workload")
	fmt.Println("========================================")
	fmt.Printf("Key:         %s\n", keyValue)
	fmt.Printf("Namespace:   %s\n", c.config.Namespace)
	if c.config.Set != "" {
		fmt.Printf("Set:         %s\n", c.config.Set)
	}
	fmt.Printf("Connections: %d\n", connections)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d ops/sec per connection\n", targetRate)
		fmt.Printf("Total Rate:  ~%d ops/sec\n", targetRate*connections)
	} else {
		fmt.Printf("Target Rate: Unlimited (max throughput)\n")
	}
	fmt.Println("========================================")
	fmt.Print("\nStarting workload in 3 seconds... Press Ctrl+C to stop early\n")
	time.Sleep(3 * time.Second)

	// Statistics tracking
	type Stats struct {
		success int64
		errors  int64
		timeout int64
	}
	
	var stats Stats
	var mu sync.Mutex
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			localSuccess := int64(0)
			localErrors := int64(0)
			localTimeout := int64(0)
			
			// Rate limiting setup
			var ticker *time.Ticker
			var tickChan <-chan time.Time
			if targetRate > 0 {
				interval := time.Second / time.Duration(targetRate)
				ticker = time.NewTicker(interval)
				tickChan = ticker.C
				defer ticker.Stop()
			}

			for {
				select {
				case <-stopChan:
					mu.Lock()
					stats.success += localSuccess
					stats.errors += localErrors
					stats.timeout += localTimeout
					mu.Unlock()
					return
				default:
					// Rate limit if configured
					if targetRate > 0 {
						<-tickChan
					}
					
					// Perform read
					_, err := c.client.Get(policy, key)
					if err != nil {
						if aerr, ok := err.(*aero.AerospikeError); ok {
							if strings.Contains(strings.ToLower(aerr.Error()), "timeout") {
								localTimeout++
							} else {
								localErrors++
							}
						} else {
							localErrors++
						}
					} else {
						localSuccess++
					}
				}
			}
		}(i)
	}

	// Progress reporting
	startTime := time.Now()
	endTime := startTime.Add(time.Duration(duration) * time.Second)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("\nRunning... (updates every 5 seconds)")
	fmt.Println("----------------------------------------")

	for time.Now().Before(endTime) {
		select {
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()
			mu.Lock()
			currentSuccess := stats.success
			currentErrors := stats.errors
			currentTimeout := stats.timeout
			mu.Unlock()
			
			total := currentSuccess + currentErrors + currentTimeout
			opsPerSec := float64(total) / elapsed
			
			fmt.Printf("[%.0fs] Total: %d | Success: %d | Errors: %d | Timeouts: %d | Rate: %.0f ops/sec\n",
				elapsed, total, currentSuccess, currentErrors, currentTimeout, opsPerSec)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Stop all workers
	close(stopChan)
	wg.Wait()

	// Final statistics
	totalDuration := time.Since(startTime).Seconds()
	totalOps := stats.success + stats.errors + stats.timeout
	avgRate := float64(totalOps) / totalDuration

	fmt.Println("\n========================================")
	fmt.Println("         Workload Complete")
	fmt.Println("========================================")
	fmt.Printf("Duration:        %.2f seconds\n", totalDuration)
	fmt.Printf("Total Ops:       %d\n", totalOps)
	fmt.Printf("Successful:      %d (%.2f%%)\n", stats.success, float64(stats.success)/float64(totalOps)*100)
	fmt.Printf("Errors:          %d (%.2f%%)\n", stats.errors, float64(stats.errors)/float64(totalOps)*100)
	fmt.Printf("Timeouts:        %d (%.2f%%)\n", stats.timeout, float64(stats.timeout)/float64(totalOps)*100)
	fmt.Printf("Average Rate:    %.0f ops/sec\n", avgRate)
	fmt.Printf("Per Connection:  %.0f ops/sec\n", avgRate/float64(connections))
	fmt.Println("========================================")
}

func (c *CLI) handleHotPut(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: hotput <key> [connections] [duration] [rate]")
		fmt.Println("\nGenerate a high-rate write workload on a single key")
		fmt.Println("\nParameters:")
		fmt.Println("  key          - Key to write repeatedly")
		fmt.Println("  connections  - Number of concurrent connections (default: 100)")
		fmt.Println("  duration     - Test duration in seconds (default: 60)")
		fmt.Println("  rate         - Target ops/sec per connection (default: 1000, 0=unlimited)")
		fmt.Println("\nExample:")
		fmt.Println("  hotput user1 200 30 5000")
		fmt.Println("  (200 connections, 30 seconds, 5000 ops/sec per connection)")
		fmt.Println("\nNote: Each write increments a counter bin to create unique writes")
		return
	}

	keyValue := args[0]
	
	// Parse optional parameters
	connections := 100
	duration := 60
	targetRate := 1000
	
	if len(args) > 1 {
		if c, err := strconv.Atoi(args[1]); err == nil {
			connections = c
		}
	}
	if len(args) > 2 {
		if d, err := strconv.Atoi(args[2]); err == nil {
			duration = d
		}
	}
	if len(args) > 3 {
		if r, err := strconv.Atoi(args[3]); err == nil {
			targetRate = r
		}
	}

	// Create the key
	key, err := aero.NewKey(c.config.Namespace, c.config.Set, keyValue)
	if err != nil {
		c.logError("Error creating key", err)
		return
	}

	fmt.Println("\n========================================")
	fmt.Println("         Hot Key Write Workload")
	fmt.Println("========================================")
	fmt.Printf("Key:         %s\n", keyValue)
	fmt.Printf("Namespace:   %s\n", c.config.Namespace)
	if c.config.Set != "" {
		fmt.Printf("Set:         %s\n", c.config.Set)
	}
	fmt.Printf("Connections: %d\n", connections)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d ops/sec per connection\n", targetRate)
		fmt.Printf("Total Rate:  ~%d ops/sec\n", targetRate*connections)
	} else {
		fmt.Printf("Target Rate: Unlimited (max throughput)\n")
	}
	fmt.Println("========================================")
	fmt.Print("\nStarting workload in 3 seconds... Press Ctrl+C to stop early\n")
	time.Sleep(3 * time.Second)

	// Statistics tracking
	type Stats struct {
		success int64
		errors  int64
		timeout int64
	}
	
	var stats Stats
	var mu sync.Mutex
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			localSuccess := int64(0)
			localErrors := int64(0)
			localTimeout := int64(0)
			counter := int64(0)
			
			// Rate limiting setup
			var ticker *time.Ticker
			var tickChan <-chan time.Time
			if targetRate > 0 {
				interval := time.Second / time.Duration(targetRate)
				ticker = time.NewTicker(interval)
				tickChan = ticker.C
				defer ticker.Stop()
			}

			writePolicy := c.getWritePolicy()

			for {
				select {
				case <-stopChan:
					mu.Lock()
					stats.success += localSuccess
					stats.errors += localErrors
					stats.timeout += localTimeout
					mu.Unlock()
					return
				default:
					// Rate limit if configured
					if targetRate > 0 {
						<-tickChan
					}
					
					// Perform write with incrementing counter
					counter++
					bins := aero.BinMap{
						"counter":   counter,
						"worker_id": workerID,
						"timestamp": time.Now().Unix(),
					}
					
					err := c.client.Put(writePolicy, key, bins)
					if err != nil {
						if aerr, ok := err.(*aero.AerospikeError); ok {
							if strings.Contains(strings.ToLower(aerr.Error()), "timeout") {
								localTimeout++
							} else {
								localErrors++
							}
						} else {
							localErrors++
						}
					} else {
						localSuccess++
					}
				}
			}
		}(i)
	}

	// Progress reporting
	startTime := time.Now()
	endTime := startTime.Add(time.Duration(duration) * time.Second)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("\nRunning... (updates every 5 seconds)")
	fmt.Println("----------------------------------------")

	for time.Now().Before(endTime) {
		select {
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()
			mu.Lock()
			currentSuccess := stats.success
			currentErrors := stats.errors
			currentTimeout := stats.timeout
			mu.Unlock()
			
			total := currentSuccess + currentErrors + currentTimeout
			opsPerSec := float64(total) / elapsed
			
			fmt.Printf("[%.0fs] Total: %d | Success: %d | Errors: %d | Timeouts: %d | Rate: %.0f ops/sec\n",
				elapsed, total, currentSuccess, currentErrors, currentTimeout, opsPerSec)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Stop all workers
	close(stopChan)
	wg.Wait()

	// Final statistics
	totalDuration := time.Since(startTime).Seconds()
	totalOps := stats.success + stats.errors + stats.timeout
	avgRate := float64(totalOps) / totalDuration

	fmt.Println("\n========================================")
	fmt.Println("         Workload Complete")
	fmt.Println("========================================")
	fmt.Printf("Duration:        %.2f seconds\n", totalDuration)
	fmt.Printf("Total Ops:       %d\n", totalOps)
	fmt.Printf("Successful:      %d (%.2f%%)\n", stats.success, float64(stats.success)/float64(totalOps)*100)
	fmt.Printf("Errors:          %d (%.2f%%)\n", stats.errors, float64(stats.errors)/float64(totalOps)*100)
	fmt.Printf("Timeouts:        %d (%.2f%%)\n", stats.timeout, float64(stats.timeout)/float64(totalOps)*100)
	fmt.Printf("Average Rate:    %.0f ops/sec\n", avgRate)
	fmt.Printf("Per Connection:  %.0f ops/sec\n", avgRate/float64(connections))
	fmt.Println("========================================")
	
	// Show final record state
	fmt.Println("\nFinal record state:")
	readPolicy := c.getReadPolicy()
	record, err := c.client.Get(readPolicy, key)
	if err == nil && record != nil {
		fmt.Printf("  Generation: %d\n", record.Generation)
		for binName, binValue := range record.Bins {
			fmt.Printf("  %s: %v\n", binName, binValue)
		}
	}
}

// Load Test Handlers

func (c *CLI) handleLoadTestRead(args []string) {
	// Parse parameters
	keyPrefix := "loadtest"
	numKeys := 1000
	threads := 50
	duration := 60
	targetRate := 100

	if len(args) > 0 {
		keyPrefix = args[0]
	}
	if len(args) > 1 {
		if n, err := strconv.Atoi(args[1]); err == nil {
			numKeys = n
		}
	}
	if len(args) > 2 {
		if t, err := strconv.Atoi(args[2]); err == nil {
			threads = t
		}
	}
	if len(args) > 3 {
		if d, err := strconv.Atoi(args[3]); err == nil {
			duration = d
		}
	}
	if len(args) > 4 {
		if r, err := strconv.Atoi(args[4]); err == nil {
			targetRate = r
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("      Load Test - Distributed Reads")
	fmt.Println("========================================")
	fmt.Printf("Key Prefix:  %s\n", keyPrefix)
	fmt.Printf("Num Keys:    %d\n", numKeys)
	fmt.Printf("Threads:     %d\n", threads)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d ops/sec per thread\n", targetRate)
		fmt.Printf("Total Rate:  ~%d ops/sec\n", targetRate*threads)
	} else {
		fmt.Printf("Target Rate: Unlimited\n")
	}
	fmt.Println("========================================")

	// First, ensure keys exist
	fmt.Print("\nPreparing test data... ")
	writePolicy := c.getWritePolicy()
	for i := 0; i < numKeys; i++ {
		key, _ := aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, i))
		bins := aero.BinMap{
			"id":        i,
			"data":      fmt.Sprintf("testdata_%d", i),
			"timestamp": time.Now().Unix(),
		}
		c.client.Put(writePolicy, key, bins)
		if (i+1)%100 == 0 {
			fmt.Printf(".")
		}
	}
	fmt.Println(" Done!")

	fmt.Print("\nStarting load test in 3 seconds...\n")
	time.Sleep(3 * time.Second)

	// Run load test
	stats := &LoadTestStats{
		startTime: time.Now(),
		latencies: make([]int64, 0, 10000),
	}
	
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker threads
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.loadTestReadWorker(workerID, keyPrefix, numKeys, targetRate, stopChan, stats)
		}(i)
	}

	// Progress reporting
	c.reportProgress(duration, stopChan, stats, "READ")

	// Stop workers
	close(stopChan)
	wg.Wait()

	// Final report
	c.printLoadTestReport(stats, "READ")
}

func (c *CLI) handleLoadTestWrite(args []string) {
	// Parse parameters
	keyPrefix := "loadtest"
	numKeys := 1000
	threads := 50
	duration := 60
	targetRate := 100

	if len(args) > 0 {
		keyPrefix = args[0]
	}
	if len(args) > 1 {
		if n, err := strconv.Atoi(args[1]); err == nil {
			numKeys = n
		}
	}
	if len(args) > 2 {
		if t, err := strconv.Atoi(args[2]); err == nil {
			threads = t
		}
	}
	if len(args) > 3 {
		if d, err := strconv.Atoi(args[3]); err == nil {
			duration = d
		}
	}
	if len(args) > 4 {
		if r, err := strconv.Atoi(args[4]); err == nil {
			targetRate = r
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("      Load Test - Distributed Writes")
	fmt.Println("========================================")
	fmt.Printf("Key Prefix:  %s\n", keyPrefix)
	fmt.Printf("Num Keys:    %d\n", numKeys)
	fmt.Printf("Threads:     %d\n", threads)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d ops/sec per thread\n", targetRate)
		fmt.Printf("Total Rate:  ~%d ops/sec\n", targetRate*threads)
	} else {
		fmt.Printf("Target Rate: Unlimited\n")
	}
	fmt.Println("========================================")

	fmt.Print("\nStarting load test in 3 seconds...\n")
	time.Sleep(3 * time.Second)

	// Run load test
	stats := &LoadTestStats{
		startTime: time.Now(),
		latencies: make([]int64, 0, 10000),
	}
	
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker threads
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.loadTestWriteWorker(workerID, keyPrefix, numKeys, targetRate, stopChan, stats)
		}(i)
	}

	// Progress reporting
	c.reportProgress(duration, stopChan, stats, "WRITE")

	// Stop workers
	close(stopChan)
	wg.Wait()

	// Final report
	c.printLoadTestReport(stats, "WRITE")
}

func (c *CLI) handleLoadTestBatchRead(args []string) {
	// Parse parameters
	keyPrefix := "loadtest"
	numKeys := 1000
	batchSize := 10
	threads := 50
	duration := 60
	targetRate := 10 // Lower default for batch ops

	if len(args) > 0 {
		keyPrefix = args[0]
	}
	if len(args) > 1 {
		if n, err := strconv.Atoi(args[1]); err == nil {
			numKeys = n
		}
	}
	if len(args) > 2 {
		if b, err := strconv.Atoi(args[2]); err == nil {
			batchSize = b
		}
	}
	if len(args) > 3 {
		if t, err := strconv.Atoi(args[3]); err == nil {
			threads = t
		}
	}
	if len(args) > 4 {
		if d, err := strconv.Atoi(args[4]); err == nil {
			duration = d
		}
	}
	if len(args) > 5 {
		if r, err := strconv.Atoi(args[5]); err == nil {
			targetRate = r
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("     Load Test - Batch Reads")
	fmt.Println("========================================")
	fmt.Printf("Key Prefix:  %s\n", keyPrefix)
	fmt.Printf("Num Keys:    %d\n", numKeys)
	fmt.Printf("Batch Size:  %d\n", batchSize)
	fmt.Printf("Threads:     %d\n", threads)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d batch ops/sec per thread\n", targetRate)
		fmt.Printf("Total Rate:  ~%d batch ops/sec (~%d individual reads/sec)\n", 
			targetRate*threads, targetRate*threads*batchSize)
	} else {
		fmt.Printf("Target Rate: Unlimited\n")
	}
	fmt.Println("========================================")

	// Prepare test data
	fmt.Print("\nPreparing test data... ")
	writePolicy := c.getWritePolicy()
	for i := 0; i < numKeys; i++ {
		key, _ := aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, i))
		bins := aero.BinMap{
			"id":        i,
			"data":      fmt.Sprintf("testdata_%d", i),
			"timestamp": time.Now().Unix(),
		}
		c.client.Put(writePolicy, key, bins)
		if (i+1)%100 == 0 {
			fmt.Printf(".")
		}
	}
	fmt.Println(" Done!")

	fmt.Print("\nStarting load test in 3 seconds...\n")
	time.Sleep(3 * time.Second)

	// Run load test
	stats := &LoadTestStats{
		startTime: time.Now(),
		latencies: make([]int64, 0, 10000),
	}
	
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker threads
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.loadTestBatchReadWorker(workerID, keyPrefix, numKeys, batchSize, targetRate, stopChan, stats)
		}(i)
	}

	// Progress reporting
	c.reportProgress(duration, stopChan, stats, "BATCH-READ")

	// Stop workers
	close(stopChan)
	wg.Wait()

	// Final report
	c.printLoadTestReport(stats, "BATCH-READ")
}

func (c *CLI) handleLoadTestBatchWrite(args []string) {
	// Parse parameters
	keyPrefix := "loadtest"
	numKeys := 1000
	batchSize := 10
	threads := 50
	duration := 60
	targetRate := 10

	if len(args) > 0 {
		keyPrefix = args[0]
	}
	if len(args) > 1 {
		if n, err := strconv.Atoi(args[1]); err == nil {
			numKeys = n
		}
	}
	if len(args) > 2 {
		if b, err := strconv.Atoi(args[2]); err == nil {
			batchSize = b
		}
	}
	if len(args) > 3 {
		if t, err := strconv.Atoi(args[3]); err == nil {
			threads = t
		}
	}
	if len(args) > 4 {
		if d, err := strconv.Atoi(args[4]); err == nil {
			duration = d
		}
	}
	if len(args) > 5 {
		if r, err := strconv.Atoi(args[5]); err == nil {
			targetRate = r
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("     Load Test - Batch Writes")
	fmt.Println("========================================")
	fmt.Printf("Key Prefix:  %s\n", keyPrefix)
	fmt.Printf("Num Keys:    %d\n", numKeys)
	fmt.Printf("Batch Size:  %d\n", batchSize)
	fmt.Printf("Threads:     %d\n", threads)
	fmt.Printf("Duration:    %d seconds\n", duration)
	if targetRate > 0 {
		fmt.Printf("Target Rate: %d batch ops/sec per thread\n", targetRate)
		fmt.Printf("Total Rate:  ~%d batch ops/sec (~%d individual writes/sec)\n", 
			targetRate*threads, targetRate*threads*batchSize)
	} else {
		fmt.Printf("Target Rate: Unlimited\n")
	}
	fmt.Println("========================================")

	fmt.Print("\nStarting load test in 3 seconds...\n")
	time.Sleep(3 * time.Second)

	// Run load test
	stats := &LoadTestStats{
		startTime: time.Now(),
		latencies: make([]int64, 0, 10000),
	}
	
	stopChan := make(chan bool)
	var wg sync.WaitGroup

	// Start worker threads
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.loadTestBatchWriteWorker(workerID, keyPrefix, numKeys, batchSize, targetRate, stopChan, stats)
		}(i)
	}

	// Progress reporting
	c.reportProgress(duration, stopChan, stats, "BATCH-WRITE")

	// Stop workers
	close(stopChan)
	wg.Wait()

	// Final report
	c.printLoadTestReport(stats, "BATCH-WRITE")
}

// Load Test Worker Functions

func (c *CLI) loadTestReadWorker(workerID int, keyPrefix string, numKeys int, targetRate int, stopChan chan bool, stats *LoadTestStats) {
	policy := c.getReadPolicy()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	
	var ticker *time.Ticker
	var tickChan <-chan time.Time
	if targetRate > 0 {
		interval := time.Second / time.Duration(targetRate)
		ticker = time.NewTicker(interval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-stopChan:
			return
		default:
			if targetRate > 0 {
				<-tickChan
			}

			keyNum := rnd.Intn(numKeys)
			key, _ := aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, keyNum))
			
			start := time.Now()
			_, err := c.client.Get(policy, key)
			latency := time.Since(start).Microseconds()

			atomic.AddInt64(&stats.totalOps, 1)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "timeout") {
					atomic.AddInt64(&stats.timeoutOps, 1)
				} else {
					atomic.AddInt64(&stats.errorOps, 1)
				}
			} else {
				atomic.AddInt64(&stats.successOps, 1)
				stats.recordLatency(latency)
			}
		}
	}
}

func (c *CLI) loadTestWriteWorker(workerID int, keyPrefix string, numKeys int, targetRate int, stopChan chan bool, stats *LoadTestStats) {
	policy := c.getWritePolicy()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	
	var ticker *time.Ticker
	var tickChan <-chan time.Time
	if targetRate > 0 {
		interval := time.Second / time.Duration(targetRate)
		ticker = time.NewTicker(interval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	counter := int64(0)
	for {
		select {
		case <-stopChan:
			return
		default:
			if targetRate > 0 {
				<-tickChan
			}

			keyNum := rnd.Intn(numKeys)
			key, _ := aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, keyNum))
			
			counter++
			bins := aero.BinMap{
				"id":        keyNum,
				"counter":   counter,
				"worker_id": workerID,
				"timestamp": time.Now().Unix(),
				"data":      fmt.Sprintf("data_%d_%d", workerID, counter),
			}
			
			start := time.Now()
			err := c.client.Put(policy, key, bins)
			latency := time.Since(start).Microseconds()

			atomic.AddInt64(&stats.totalOps, 1)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "timeout") {
					atomic.AddInt64(&stats.timeoutOps, 1)
				} else {
					atomic.AddInt64(&stats.errorOps, 1)
				}
			} else {
				atomic.AddInt64(&stats.successOps, 1)
				stats.recordLatency(latency)
			}
		}
	}
}

func (c *CLI) loadTestBatchReadWorker(workerID int, keyPrefix string, numKeys int, batchSize int, targetRate int, stopChan chan bool, stats *LoadTestStats) {
	policy := c.getBatchPolicy()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	
	var ticker *time.Ticker
	var tickChan <-chan time.Time
	if targetRate > 0 {
		interval := time.Second / time.Duration(targetRate)
		ticker = time.NewTicker(interval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-stopChan:
			return
		default:
			if targetRate > 0 {
				<-tickChan
			}

			// Create batch of random keys
			keys := make([]*aero.Key, batchSize)
			for i := 0; i < batchSize; i++ {
				keyNum := rnd.Intn(numKeys)
				keys[i], _ = aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, keyNum))
			}
			
			start := time.Now()
			_, err := c.client.BatchGet(policy, keys)
			latency := time.Since(start).Microseconds()

			atomic.AddInt64(&stats.totalOps, 1)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "timeout") {
					atomic.AddInt64(&stats.timeoutOps, 1)
				} else {
					atomic.AddInt64(&stats.errorOps, 1)
				}
			} else {
				atomic.AddInt64(&stats.successOps, 1)
				stats.recordLatency(latency)
			}
		}
	}
}

func (c *CLI) loadTestBatchWriteWorker(workerID int, keyPrefix string, numKeys int, batchSize int, targetRate int, stopChan chan bool, stats *LoadTestStats) {
	policy := c.getWritePolicy()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	
	var ticker *time.Ticker
	var tickChan <-chan time.Time
	if targetRate > 0 {
		interval := time.Second / time.Duration(targetRate)
		ticker = time.NewTicker(interval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	counter := int64(0)
	for {
		select {
		case <-stopChan:
			return
		default:
			if targetRate > 0 {
				<-tickChan
			}

			start := time.Now()
			errors := 0
			
			// Write batch of records
			for i := 0; i < batchSize; i++ {
				keyNum := rnd.Intn(numKeys)
				key, _ := aero.NewKey(c.config.Namespace, c.config.Set, fmt.Sprintf("%s_%d", keyPrefix, keyNum))
				
				counter++
				bins := aero.BinMap{
					"id":        keyNum,
					"counter":   counter,
					"worker_id": workerID,
					"batch_id":  i,
					"timestamp": time.Now().Unix(),
					"data":      fmt.Sprintf("data_%d_%d_%d", workerID, counter, i),
				}
				
				err := c.client.Put(policy, key, bins)
				if err != nil {
					errors++
				}
			}
			
			latency := time.Since(start).Microseconds()

			atomic.AddInt64(&stats.totalOps, 1)
			if errors == batchSize {
				atomic.AddInt64(&stats.errorOps, 1)
			} else if errors > 0 {
				atomic.AddInt64(&stats.errorOps, 1)
			} else {
				atomic.AddInt64(&stats.successOps, 1)
				stats.recordLatency(latency)
			}
		}
	}
}

func (c *CLI) reportProgress(duration int, stopChan chan bool, stats *LoadTestStats, opType string) {
	endTime := stats.startTime.Add(time.Duration(duration) * time.Second)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("\nRunning... (updates every 5 seconds)")
	fmt.Println("----------------------------------------")

	for time.Now().Before(endTime) {
		select {
		case <-ticker.C:
			elapsed := time.Since(stats.startTime).Seconds()
			total := atomic.LoadInt64(&stats.totalOps)
			success := atomic.LoadInt64(&stats.successOps)
			errors := atomic.LoadInt64(&stats.errorOps)
			timeouts := atomic.LoadInt64(&stats.timeoutOps)
			
			opsPerSec := float64(total) / elapsed
			
			fmt.Printf("[%.0fs] Ops: %d | Success: %d | Errors: %d | Timeouts: %d | Rate: %.0f ops/sec\n",
				elapsed, total, success, errors, timeouts, opsPerSec)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *CLI) printLoadTestReport(stats *LoadTestStats, opType string) {
	totalDuration := time.Since(stats.startTime).Seconds()
	total := atomic.LoadInt64(&stats.totalOps)
	success := atomic.LoadInt64(&stats.successOps)
	errors := atomic.LoadInt64(&stats.errorOps)
	timeouts := atomic.LoadInt64(&stats.timeoutOps)
	
	avgRate := float64(total) / totalDuration
	
	p50, p95, p99, pMax := stats.getPercentiles()

	fmt.Println("\n========================================")
	fmt.Printf("      %s Load Test Complete\n", opType)
	fmt.Println("========================================")
	fmt.Printf("Duration:        %.2f seconds\n", totalDuration)
	fmt.Printf("Total Ops:       %d\n", total)
	fmt.Printf("Successful:      %d (%.2f%%)\n", success, float64(success)/float64(total)*100)
	fmt.Printf("Errors:          %d (%.2f%%)\n", errors, float64(errors)/float64(total)*100)
	fmt.Printf("Timeouts:        %d (%.2f%%)\n", timeouts, float64(timeouts)/float64(total)*100)
	fmt.Printf("Average Rate:    %.0f ops/sec\n", avgRate)
	
	if len(stats.latencies) > 0 {
		fmt.Println("\nLatency Distribution (microseconds):")
		fmt.Printf("  P50:  %d µs (%.2f ms)\n", p50, float64(p50)/1000)
		fmt.Printf("  P95:  %d µs (%.2f ms)\n", p95, float64(p95)/1000)
		fmt.Printf("  P99:  %d µs (%.2f ms)\n", p99, float64(p99)/1000)
		fmt.Printf("  Max:  %d µs (%.2f ms)\n", pMax, float64(pMax)/1000)
	}
	fmt.Println("========================================")
}

// Add these functions to the end of your main.go file after the load test functions

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

func (c *CLI) handleShowIndexes(args []string) {
	// Get all nodes in the cluster
	nodes := c.client.GetNodes()
	if len(nodes) == 0 {
		fmt.Println("No nodes available in cluster")
		return
	}

	// Query index information from the first node
	node := nodes[0]
	
	// Use Info command to get sindex information
	policy := aero.NewInfoPolicy()
	infoMap, err := node.RequestInfo(policy, "sindex")
	if err != nil {
		c.logError("Failed to retrieve index information", err)
		return
	}

	sindexInfo, exists := infoMap["sindex"]
	if !exists || sindexInfo == "" {
		fmt.Println("No secondary indexes found")
		return
	}

	// Parse the sindex response
	// Format: ns=<namespace>:set=<set>:indexname=<name>:num_bins=1:bins=<bin>:type=<type>:indextype=<indextype>:state=<state>;...
	indexes := strings.Split(sindexInfo, ";")
	
	if len(indexes) == 0 || (len(indexes) == 1 && indexes[0] == "") {
		fmt.Println("No secondary indexes found")
		return
	}

	fmt.Println("\nSecondary Indexes:")
	fmt.Println("========================================")
	
	count := 0
	for _, indexStr := range indexes {
		if strings.TrimSpace(indexStr) == "" {
			continue
		}
		
		count++
		indexData := make(map[string]string)
		
		// Parse key=value pairs
		pairs := strings.Split(indexStr, ":")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				indexData[kv[0]] = kv[1]
			}
		}
		
		// Display index information
		fmt.Printf("\n[%d] Index: %s\n", count, indexData["indexname"])
		fmt.Printf("    Namespace: %s\n", indexData["ns"])
		if set, exists := indexData["set"]; exists && set != "NULL" {
			fmt.Printf("    Set:       %s\n", set)
		} else {
			fmt.Printf("    Set:       (none)\n")
		}
		fmt.Printf("    Bin:       %s\n", indexData["bins"])
		fmt.Printf("    Type:      %s\n", indexData["type"])
		fmt.Printf("    State:     %s\n", indexData["state"])
		
		// Show index type if available
		if indexType, exists := indexData["indextype"]; exists {
			fmt.Printf("    IndexType: %s\n", indexType)
		}
	}
	
	fmt.Printf("\nTotal indexes: %d\n", count)
	fmt.Println("========================================")
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
		fmt.Println("\n--- Connection ---")
		fmt.Printf("  Host:            %s\n", c.config.Host)
		fmt.Printf("  Port:            %d\n", c.config.Port)
		fmt.Printf("  Namespace:       %s\n", c.config.Namespace)
		fmt.Printf("  Set:             %s\n", c.config.Set)
		fmt.Printf("  Debug:           %v\n", c.config.Debug)
		
		fmt.Println("\n--- Timeout Policies ---")
		fmt.Printf("  SocketTimeout:   %v\n", c.config.SocketTimeout)
		fmt.Printf("  TotalTimeout:    %v\n", c.config.TotalTimeout)
		fmt.Printf("  ConnectTimeout:  %v\n", c.config.ConnectTimeout)
		fmt.Printf("  MaxRetries:      %d\n", c.config.MaxRetries)
		
		fmt.Println("\n--- Write Policies ---")
		fmt.Printf("  RecordExistsAction: %s\n", c.config.RecordExistsAction)
		fmt.Printf("  GenerationPolicy:   %s\n", c.config.GenerationPolicy)
		fmt.Printf("  Generation:         %d\n", c.config.Generation)
		fmt.Printf("  Expiration:         %d\n", c.config.Expiration)
		fmt.Printf("  DurableDelete:      %v\n", c.config.DurableDelete)
		fmt.Printf("  SendKey:            %v\n", c.config.SendKey)
		fmt.Printf("  CommitLevel:        %s\n", c.config.CommitLevel)
		
		fmt.Println("\n--- Read Policies ---")
		fmt.Printf("  ReadModeAP:      %s\n", c.config.ReadModeAP)
		fmt.Printf("  ReadModeSC:      %s\n", c.config.ReadModeSC)
		fmt.Printf("  Replica:         %s\n", c.config.Replica)
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
		case "record-exists-action":
			upper := strings.ToUpper(value)
			if upper == "UPDATE" || upper == "UPDATE_ONLY" || upper == "REPLACE" || 
			   upper == "REPLACE_ONLY" || upper == "CREATE_ONLY" {
				c.config.RecordExistsAction = upper
				fmt.Printf("RecordExistsAction set to: %s\n", c.config.RecordExistsAction)
			} else {
				fmt.Println("Invalid value. Use: UPDATE, UPDATE_ONLY, REPLACE, REPLACE_ONLY, CREATE_ONLY")
			}
		case "generation-policy":
			upper := strings.ToUpper(value)
			if upper == "NONE" || upper == "EXPECT_GEN_EQUAL" || upper == "EXPECT_GEN_GT" {
				c.config.GenerationPolicy = upper
				fmt.Printf("GenerationPolicy set to: %s\n", c.config.GenerationPolicy)
			} else {
				fmt.Println("Invalid value. Use: NONE, EXPECT_GEN_EQUAL, EXPECT_GEN_GT")
			}
		case "generation":
			gen, _ := strconv.ParseUint(value, 10, 32)
			c.config.Generation = uint32(gen)
			fmt.Printf("Generation set to: %d\n", c.config.Generation)
		case "expiration", "ttl":
			exp, _ := strconv.ParseInt(value, 10, 32)
			c.config.Expiration = int32(exp)
			fmt.Printf("Expiration (TTL) set to: %d\n", c.config.Expiration)
		case "durable-delete":
			c.config.DurableDelete = value == "true"
			fmt.Printf("DurableDelete set to: %v\n", c.config.DurableDelete)
		case "send-key":
			c.config.SendKey = value == "true"
			fmt.Printf("SendKey set to: %v\n", c.config.SendKey)
		case "commit-level":
			upper := strings.ToUpper(value)
			if upper == "COMMIT_ALL" || upper == "COMMIT_MASTER" {
				c.config.CommitLevel = upper
				fmt.Printf("CommitLevel set to: %s\n", c.config.CommitLevel)
			} else {
				fmt.Println("Invalid value. Use: COMMIT_ALL, COMMIT_MASTER")
			}
		case "read-mode-ap":
			upper := strings.ToUpper(value)
			if upper == "ONE" || upper == "ALL" {
				c.config.ReadModeAP = upper
				fmt.Printf("ReadModeAP set to: %s\n", c.config.ReadModeAP)
			} else {
				fmt.Println("Invalid value. Use: ONE, ALL")
			}
		case "read-mode-sc":
			upper := strings.ToUpper(value)
			if upper == "SESSION" || upper == "LINEARIZE" || upper == "ALLOW_REPLICA" || upper == "ALLOW_UNAVAILABLE" {
				c.config.ReadModeSC = upper
				fmt.Printf("ReadModeSC set to: %s\n", c.config.ReadModeSC)
			} else {
				fmt.Println("Invalid value. Use: SESSION, LINEARIZE, ALLOW_REPLICA, ALLOW_UNAVAILABLE")
			}
		case "replica":
			upper := strings.ToUpper(value)
			if upper == "MASTER" || upper == "MASTER_PROLES" || upper == "RANDOM" || 
			   upper == "SEQUENCE" || upper == "PREFER_RACK" {
				c.config.Replica = upper
				fmt.Printf("Replica set to: %s\n", c.config.Replica)
			} else {
				fmt.Println("Invalid value. Use: MASTER, MASTER_PROLES, RANDOM, SEQUENCE, PREFER_RACK")
			}
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

// Policy Functions

func (c *CLI) getWritePolicy() *aero.WritePolicy {
	policy := aero.NewWritePolicy(0, uint32(c.config.Expiration))
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	
	// Set RecordExistsAction
	switch c.config.RecordExistsAction {
	case "UPDATE":
		policy.RecordExistsAction = aero.UPDATE
	case "UPDATE_ONLY":
		policy.RecordExistsAction = aero.UPDATE_ONLY
	case "REPLACE":
		policy.RecordExistsAction = aero.REPLACE
	case "REPLACE_ONLY":
		policy.RecordExistsAction = aero.REPLACE_ONLY
	case "CREATE_ONLY":
		policy.RecordExistsAction = aero.CREATE_ONLY
	}
	
	// Set GenerationPolicy
	switch c.config.GenerationPolicy {
	case "NONE":
		policy.GenerationPolicy = aero.NONE
	case "EXPECT_GEN_EQUAL":
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = c.config.Generation
	case "EXPECT_GEN_GT":
		policy.GenerationPolicy = aero.EXPECT_GEN_GT
		policy.Generation = c.config.Generation
	}
	
	// Set CommitLevel
	switch c.config.CommitLevel {
	case "COMMIT_ALL":
		policy.CommitLevel = aero.COMMIT_ALL
	case "COMMIT_MASTER":
		policy.CommitLevel = aero.COMMIT_MASTER
	}
	
	// Set DurableDelete
	policy.DurableDelete = c.config.DurableDelete
	
	// Set SendKey
	policy.SendKey = c.config.SendKey
	
	return policy
}

func (c *CLI) getReadPolicy() *aero.BasePolicy {
	policy := aero.NewPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	
	// Set Replica
	switch c.config.Replica {
	case "MASTER":
		policy.ReplicaPolicy = aero.MASTER
	case "MASTER_PROLES":
		policy.ReplicaPolicy = aero.MASTER_PROLES
	case "RANDOM":
		policy.ReplicaPolicy = aero.RANDOM
	case "SEQUENCE":
		policy.ReplicaPolicy = aero.SEQUENCE
	case "PREFER_RACK":
		policy.ReplicaPolicy = aero.PREFER_RACK
	}
	
	// Set ReadModeAP
	switch c.config.ReadModeAP {
	case "ONE":
		policy.ReadModeAP = aero.ReadModeAPOne
	case "ALL":
		policy.ReadModeAP = aero.ReadModeAPAll
	}
	
	// Set ReadModeSC
	switch c.config.ReadModeSC {
	case "SESSION":
		policy.ReadModeSC = aero.ReadModeSCSession
	case "LINEARIZE":
		policy.ReadModeSC = aero.ReadModeSCLinearize
	case "ALLOW_REPLICA":
		policy.ReadModeSC = aero.ReadModeSCAllowReplica
	case "ALLOW_UNAVAILABLE":
		policy.ReadModeSC = aero.ReadModeSCAllowUnavailable
	}
	
	return policy
}

func (c *CLI) getQueryPolicy() *aero.QueryPolicy {
	policy := aero.NewQueryPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	
	// Set Replica
	switch c.config.Replica {
	case "MASTER":
		policy.ReplicaPolicy = aero.MASTER
	case "MASTER_PROLES":
		policy.ReplicaPolicy = aero.MASTER_PROLES
	case "RANDOM":
		policy.ReplicaPolicy = aero.RANDOM
	case "SEQUENCE":
		policy.ReplicaPolicy = aero.SEQUENCE
	case "PREFER_RACK":
		policy.ReplicaPolicy = aero.PREFER_RACK
	}
	
	return policy
}

func (c *CLI) getScanPolicy() *aero.ScanPolicy {
	policy := aero.NewScanPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	
	// Set Replica
	switch c.config.Replica {
	case "MASTER":
		policy.ReplicaPolicy = aero.MASTER
	case "MASTER_PROLES":
		policy.ReplicaPolicy = aero.MASTER_PROLES
	case "RANDOM":
		policy.ReplicaPolicy = aero.RANDOM
	case "SEQUENCE":
		policy.ReplicaPolicy = aero.SEQUENCE
	case "PREFER_RACK":
		policy.ReplicaPolicy = aero.PREFER_RACK
	}
	
	return policy
}

func (c *CLI) getBatchPolicy() *aero.BatchPolicy {
	policy := aero.NewBatchPolicy()
	policy.SocketTimeout = c.config.SocketTimeout
	policy.TotalTimeout = c.config.TotalTimeout
	policy.MaxRetries = c.config.MaxRetries
	
	// Set Replica
	switch c.config.Replica {
	case "MASTER":
		policy.ReplicaPolicy = aero.MASTER
	case "MASTER_PROLES":
		policy.ReplicaPolicy = aero.MASTER_PROLES
	case "RANDOM":
		policy.ReplicaPolicy = aero.RANDOM
	case "SEQUENCE":
		policy.ReplicaPolicy = aero.SEQUENCE
	case "PREFER_RACK":
		policy.ReplicaPolicy = aero.PREFER_RACK
	}
	
	return policy
}

// Utility Functions

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
	
	// Convert expiration to readable format
	expirationStr := fmt.Sprintf("%d", record.Expiration)
	if record.Expiration == 0 {
		expirationStr = "0 (never expires)"
	} else if record.Expiration == 4294967295 { // Max uint32 - special "never expire" value
		expirationStr = "4294967295 (never expires)"
	} else if record.Expiration > 0 {
		// Aerospike expiration is in seconds since Citrusleaf epoch (2010-01-01)
		aerospikeEpoch := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
		expirationTime := aerospikeEpoch.Add(time.Duration(record.Expiration) * time.Second)
		
		// Calculate TTL remaining
		now := time.Now().UTC()
		ttlRemaining := expirationTime.Sub(now)
		
		if ttlRemaining > 0 {
			expirationStr = fmt.Sprintf("%d (%s, TTL: %s)", 
				record.Expiration, 
				expirationTime.Format("2006-01-02 15:04:05 MST"),
				formatDuration(ttlRemaining))
		} else {
			expirationStr = fmt.Sprintf("%d (%s, expired)", 
				record.Expiration, 
				expirationTime.Format("2006-01-02 15:04:05 MST"))
		}
	}
	
	fmt.Printf("Generation: %d, Expiration: %s\n", record.Generation, expirationStr)
	fmt.Println("Bins:")
	for name, value := range record.Bins {
		fmt.Printf("  %s: %v (%T)\n", name, value, value)
	}
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "expired"
	}
	
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, minutes, seconds)
	} else if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
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
