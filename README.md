# CS4225 Homework 4 - Distributed System with Lamport Clocks

## Project Overview
This project implements a distributed system where multiple nodes communicate by sending events to each other. Each node maintains a Lamport clock for logical time synchronization across the system. The implementation includes a suboptimal (S) version that serves as a baseline for performance comparison.

## Files Structure

### Core Files
- **NodeS.java** - Suboptimal version implementation (platform threads, no optimizations)
- **LamportClock.java** - Lamport clock for logical time synchronization
- **Event.java** - Event class for inter-node communication (provided by CS4225)
- **NodeInfo.java** - Simple data container for remote node information

### Data Files
- **nodes.csv** - Configuration file listing all nodes in the distributed system
- **README.md** - This documentation

## Requirements

### Java Version
- Java 8 or higher (platform threads)
- For optimized version: Java 21+ for virtual threads support

### Dependencies
- No external dependencies - uses only Java standard libraries

## Compilation

Compile all Java files:
```bash
javac *.java

Running the System
1. Configure nodes.csv

Create a nodes.csv file with the following format (one node per line):
NodeName1,IPAddress1
NodeName2,IPAddress2
NodeName3,IPAddress3

Example:
AnaStanescu,160.10.23.18
JakobJenkov,160.10.42.20
FeiFeiLi,160.10.42.205

2. Start Multiple Nodes

Run each node in a separate terminal window:
# Terminal 1 - Start Node 1
java NodeS AnaStanescu 160.10.23.18 200 500 25

# Terminal 2 - Start Node 2
java NodeS JakobJenkov 160.10.42.20 200 500 25

# Terminal 3 - Start Node 3
java NodeS FeiFeiLi 160.10.42.205 200 500 25

Command Line Arguments
java NodeS <nodeName> <nodeIP> <numThreads> <eventsPerThread> <remoteSendPercent>

Argument          | Description                                            | Example
nodeName          | Name of this node (must match entry in nodes.csv)      | AnaStanescu
nodeIP            | IP address of this node                                | 160.10.23.18
numThreads        | Number of worker threads to create                     | 200
eventsPerThread   | Number of events each worker thread processes          | 500
remoteSendPercent | Percentage (0-100) chance to send event to remote node | 25

Fixed Parameters
Parameter   | Value | Description
Port        | 4225  | Fixed communication port for all nodes
Random Seed | 4225  | Fixed seed for reproducible behavior

## How It Works ##
Node Initialization

    Reads nodes.csv to discover remote nodes

    Creates server socket on port 4225

    Starts shutdown listener thread waiting for 'q' command

    Initializes Lamport clock with node-specific ID

Event Processing

Each worker thread processes eventsPerThread events:

    Increments Lamport clock (time += nodeId)

    With probability remoteSendPercent:

        Selects random remote node

        Sends event via Object serialization

        Records in sent metrics

    Otherwise:

        Processes event locally

        Updates local Lamport clock

Network Communication

    Server thread accepts incoming connections

    Each connection handled in separate thread

    Events serialized using ObjectInputStream/ObjectOutputStream

    Sending events done in separate threads to avoid blocking

Shutdown

    Type 'q' in any node's terminal to initiate graceful shutdown

    Stops accepting new connections

    Waits for all worker threads to complete

    Closes server socket

    Prints final statistics

    Exits

Metrics Tracked

Each node tracks and reports:
Metric                  | Description
Total Events Processed  | Sum of local events + received events
Messages Sent           | Number of events sent to remote nodes
Messages Received       | Number of events received from remote nodes
Execution Time          | Total runtime from worker start to completion (ms)
Final Lamport Time      | Final value of the Lamport clock
Throughput              | Total events / execution time (events/sec)
Sent Distribution       | Count of events sent to each remote node
Actual Send %           | Actual percentage of events sent remotely

Output Example
=== Node AnaStanescu Statistics ===
Total events processed: 100000
Messages sent: 24987
Messages received: 25013
Execution time = 4234 ms
Final Lamport time: 423501
Throughput: 23618.32 events/sec

Events sent to remote nodes:
  JakobJenkov: 12456
  FeiFeiLi: 12531

Total local events generated: 100000
Total remote send percent configured: 25%
Actual send percentage: 24.99%

Performance Testing
Running a Single Node (for testing)
java NodeS TestNode 127.0.0.1 4 100 50

Running Multiple Nodes on Local Machine

Since all nodes use the same port (4225), you must:

    Use different IP addresses (127.0.0.1, localhost, etc.) or

    Use different network interfaces, or

    Run on different machines

For local testing, you can use different loopback addresses:
# Terminal 1
java NodeS Node1 127.0.0.1 200 500 25

# Terminal 2  
java NodeS Node2 127.0.0.2 200 500 25

# Terminal 3
java NodeS Node3 127.0.0.3 200 500 25

Key Design Decisions
Suboptimal Version (NodeS)

    Platform Threads: Uses raw Thread objects for all concurrency

    No Thread Pooling: Each worker and sender creates new threads

    Simple Synchronization: Lamport clock uses synchronized methods

    No Padding: Regular counters without cache line padding

    Fixed Seed: Ensures reproducible random behavior

Lamport Clock Implementation

    Increments by nodeId (not 1) to ensure unique timestamps

    Updates with max(current, received) + nodeId on receive

    Thread-safe using synchronized methods

Troubleshooting
"nodes.csv not found"

Ensure nodes.csv exists in the same directory as the Java class files.
"Address already in use" (BindException)

Port 4225 is already in use. Only one node per machine can run unless using different IP addresses.
No events being received

    Verify all nodes are running before workers start

    Check that nodes.csv contains all node entries

    Ensure IP addresses are correct and reachable

Slow performance

    The S version is intentionally unoptimized

    Optimized version will show significant speedup
