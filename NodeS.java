import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Suboptimal version (S) of the distrubuted node.
 * Uses platform threads, simple synchronization, no optimizations.
 *
 * Command line: java NodeS <nodeName> <nodeIP> <numThreads> <eventsPerThread> <remoteSendPercent>
 */
public class NodeS {
	// Command-line parameters
	private final String nodeName;
	private final String nodeIP;
	private final int numThreads;
	private final int eventsPerThread;
	private final int remoteSendPercent;

	// Core components
	private final LamportClock clock;
	private final List<NodeInfo> remoteNodes;
	private final Random random;
	private final int port;

	// Metrics
	private final AtomicLong totalEvents;
	private final AtomicInteger sentEvents;
	private final AtomicInteger receivedEvents;
	private final Map<String, AtomicInteger> sentToNode;

	// Thread management
	private final List<Thread> workerThreads;
	private final CountDownLatch workersLatch;
	private volatile boolean running;
	private volatile long startTime;

	// Server components
	private ServerSocket serverSocket;
	private Thread serverThread;
	private Thread shutdownListenerThread;
	
	/**
	 * Constructor for NodeS
	 * Initializes all components and loads remote nodes from nodes.csv.
	 *
	 * @param nodeName			the name of this node
	 * @param nodeIP			the IP address of this node
	 * @param numThreads		number of worker threads to create
	 * @param eventsPerThread	number of events each worker thread will process
	 * @param remoteSendPercent	percentave chanve to send event to remote node
	 */
	public NodeS(String nodeName, String nodeIP, int numThreads, int eventsPerThread, int remoteSendPercent) {
		this.nodeName = nodeName;
		this.nodeIP = nodeIP;
		this.numThreads = numThreads;
		this.eventsPerThread = eventsPerThread;
		this.remoteSendPercent = remoteSendPercent;

		// Initialize core components
		this.port = 4225;
		this.random = new Random(4225);
		int nodeId = Math.abs(nodeName.hashCode() % 100) + 1;
		this.clock = new LamportClock(nodeId);
		this.remoteNodes = new ArrayList<>();
		this.workersLatch = new CountDownLatch(numThreads);

		// Initialize metrics
		this.totalEvents = new AtomicLong(0);
		this.sentEvents = new AtomicInteger(0);
		this.receivedEvents = new AtomicInteger(0);
		this.sentToNode = new ConcurrentHashMap<>();

		// Initialize thread management
		this.workerThreads = new ArrayList<>();
		this.running = true;

		// Load remote nodes from CSV
		loadRemoteNodes();

		System.out.println("Node " + nodeName + " initialized on IP " + nodeIP);
		System.out.println("Configuration: threads = " + numThreads +
							", eventsPerThread = " + eventsPerThread + 
							", remoteSendPercent = " + remoteSendPercent + "%");
	}

	/**
	 * Main entry point for the node.
	 * Starts the server, workers, and shutdown listener, then waits for completion.
	 */
	public void start() {
		try {
			startServer();
			startShutdownListener();

			// Give server time to start
			Thread.sleep(1000);

			// Start timing
			startTime = System.currentTimeMillis();

			startWorkerThreads();

			// Wait for all workers to complete
			workersLatch.await();

			// Give time for current messages to be processed
			Thread.sleep(2000);

			// Stop accepting new connections
			running = false;

			// Wait for server thread to finish
			if (serverThread != null) {
				serverThread.join(5000);
			}

			// Calculate and print statistics
			long endTime = System.currentTimeMillis();
			printStatistics(endTime - startTime);

		} catch (InterruptedException ex) {
			System.err.println("Node " + nodeName + " interrupted");
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			System.err.println("Node " + nodeName + " error: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	/**
	 * Loads remote node information from nodes.csv file.
	 * Format: nodeName,IPAddress
	 * Skips the entry matching this node's name.
	 */
	private void loadRemoteNodes() {
		File csvFile = new File("nodes.csv");
		if (!csvFile.exists()) {
			System.err.println("Warning: nodes.csv not found in current directory");
			return;
		}

		try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
			String line;
			int lineNum = 0;
			while ((line = reader.readLine()) != null) {
				lineNum++;
				line = line.trim();
				if (line.isEmpty()) continue;

				String[] parts = line.split(",");
				if (parts.length != 2) {
					System.err.println("Warning: Invalid format at line " + lineNum + ": " + line);
					continue;
				}

				String name = parts[0].trim();
				String ip = parts[1].trim();

				if (!name.equals(nodeName)) {
					remoteNodes.add(new NodeInfo(name, ip));
					sentToNode.put(name, new AtomicInteger(0));
				}
			}
			System.out.println("Loaded " + remoteNodes.size() + " remote nodes");

		} catch (IOException ex) {
			System.err.println("Error loading nodes.csv: " + ex.getMessage());
		}
	}

	/**
	 * Starts the server thread that listens for incoming event connections.
	 * Each incoming connection is handled in a separate thread.
	 */
	private void startServer() {
		serverThread = new Thread(() -> {
			try {
				serverSocket = new ServerSocket(port);
				System.out.println("Node " + nodeName + " listening on port " + port);

				while (running) {
					try {
						Socket clientSocket = serverSocket.accept();
						Thread handlerThread = new Thread(() -> handleClient(clientSocket));
						handlerThread.start();
					} catch (SocketException ex) {
						if (running) {
							System.err.println("Server accept error: " + ex.getMessage());
						}
					}
				}
			} catch (IOException ex) {
				if (running) {
					System.err.println("Failed to start server: " + ex.getMessage());
				}
			} finally {
				closeServerSocket();
			}
		});
		serverThread.setDaemon(true);
		serverThread.start();
	}

	/**
	 * Closes the server socket gracefully.
	 */
	private void closeServerSocket() {
		try {
			if (serverSocket != null && !serverSocket.isClosed()) {
				serverSocket.close();
			}
		} catch (IOException ex) {
			// Ignore close errors
		}
	}

	/**
	 * Handles an incoming client connection by reading and processing an Event
	 *
	 * @param socket	the client socket connected to this node
	 */
	private void handleClient(Socket socket) {
		try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
			Event event = (Event) ois.readObject();

			// Update Lamport clock with received timestamp
			clock.update(event.getTimestamp());

			// Log received event
			System.out.println("RECEIVED: " + event);

			receivedEvents.incrementAndGet();
			totalEvents.incrementAndGet();

		} catch (IOException | ClassNotFoundException ex) {
			System.err.println("Error handling client: " + ex.getMessage());
		} finally {
			closeSocket(socket);
		}
	}

	/**
	 * Closes a socket safely.
	 *
	 * @param socket	the socket to close
	 */
	private void closeSocket(Socket socket) {
		try {
			if (socket != null && !socket.isClosed()) {
				socket.close();
			}
		} catch (IOException ex) {
			// Ignore close errors
		}
	}

	/**
	 * Starts the worker threads that generate and process events.
	 * Each worker thread runs independently and processes eventsPerThread events.
	 */
	private void startWorkerThreads() {
		System.out.println("Starting " + numThreads + " worker threads with " + eventsPerThread + " events each");

		for (int i = 0; i < numThreads; i++) {
			final int threadId = i;
			Thread worker = new Thread(() -> processWorkerEvents(threadId));

			worker.setName("Worker-" + threadId);
			workerThreads.add(worker);
			worker.start();
		}
	}

	/**
	 * Processes event for a single worker thread.
	 *
	 * @param threadId	the Id fo this worker thread (0-based)
	 */
	private void processWorkerEvents(int threadId) {
		try {
			for (int eventNum = 0; eventNum < eventsPerThread; eventNum++) {
				// Increment Lamport clock for local event
				long timestamp = clock.tick();

				// Determine if we send to remote or process locally
				if (shouldSendToRemote() && !remoteNodes.isEmpty()) {
					sendToRandomRemote(threadId, timestamp);
				} else {
					processLocally(threadId, timestamp);
				}

				totalEvents.incrementAndGet();

				// Small delay to simulate processing
				// simulateProcessingDelay();
			}
		} finally {
			workersLatch.countDown();
		}
	}

	/**
	 * Determines whether the current event should be sent to a remote node.
	 * Uses the configured remoteSendPercent and fixed random seed.
	 *
	 * @return true	if the event should be sent remotely
	 * 		   false otherwise
	 */
	private boolean shouldSendToRemote() {
		return random.nextInt(100) < remoteSendPercent;
	}

	/**
	 * Simulates a small processing delay to make the simulation more realistic.
	 */
	private void simulateProcessingDelay() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Sends an event to a randomly selected remote node.
	 *
	 * @param threadId		the ID of the worker thread sending the event
	 * @param timestamp		the current Lamport timestamp
	 */
	private void sendToRandomRemote(int threadId, long timestamp) {
		NodeInfo target = remoteNodes.get(random.nextInt(remoteNodes.size()));
		Event event = new Event(nodeName, target.name, timestamp);
		sendEvent(target, event);
		sentEvents.incrementAndGet();
		sentToNode.get(target.name).incrementAndGet();

		System.out.println("Thread-" + threadId + " SENT: " + event);
	}

	/**
	 * Processes an event locally.
	 *
	 * @param threadId		The ID of the worker thread processing the event
	 * @param timestamp		the current Lamport timestamp
	 */
	private void processLocally(int threadId, long timestamp) {
		Event event = new Event(nodeName, nodeName, timestamp);
		System.out.println("Thread-" + threadId + " LOCAL: " + event);
	}

	/**
	 * Sends an event to a remote node over the network.
	 * Uses a seperate thread to avoid blocking the worker.
	 *
	 * @param target	the target node information
	 * @param event		the event to send
	 */
	private void sendEvent(NodeInfo target, Event event) {
		Thread sender = new Thread(() -> {
			try (Socket socket = new Socket(target.ip, port);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
					oos.writeObject(event);
					oos.flush();
				} catch (IOException ex) {
					System.err.println("Failed to send event to " + target.name + ": " + ex.getMessage());
				}
		});
		sender.setDaemon(true);
		sender.start();
	}

	/**
	 * Starts the shutdown listener thread that waits for 'q' command from stdin.
	 * When 'q' is received, initiates graceful shutdown.
	 */
	private void startShutdownListener() {
		shutdownListenerThread = new Thread(() -> {
			try (Scanner scanner = new Scanner(System.in)) {
				while (true) {
					String input = scanner.nextLine().trim();
					if (input.equalsIgnoreCase("q")) {
						performGracefulShutdown();
						break;
					}
				}
			}
		});
		shutdownListenerThread.setDaemon(true);
		shutdownListenerThread.start();
	}

	/**
	 * Performs graceful shutdown:
	 * - Stops accepting new connections
	 * - Waits for workers to complete
	 * - Closes server socket
	 * - Prints final statistics
	 */
	private void performGracefulShutdown() {
		System.out.println("\nShutdown command received. Stopping...");
		running = false;

		// Wait for workers to finish
		try {
			workersLatch.await();
			System.out.println("All worker threads completed.");
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}

		// Close server socket
		closeServerSocket();

		// Print final stats before exit
		long endTime = System.currentTimeMillis();
		printStatistics(endTime - startTime);
		System.out.println("Node " + nodeName + " shutdown complete.");
		System.exit(0);
	}

	/**
	 * Prints final statistics for this node.
	 * Includes execution time, event counts, throughput, and Lamport timestamp.
	 *
	 * @param executionTime	the total execution time im milliseconds
	 */
	private void printStatistics(long executionTime) {
		double throughput = totalEvents.get() / (executionTime / 1000.0);

        System.out.println("\n=== Node " + nodeName + " Statistics ===");
        System.out.println("Total events processed: " + totalEvents.get());
        System.out.println("Messages sent: " + sentEvents.get());
        System.out.println("Messages received: " + receivedEvents.get());
        System.out.println("Execution time = " + executionTime + " ms");
        System.out.println("Final Lamport time: " + clock.getTime());
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " events/sec");

        System.out.println("\nEvents sent to remote nodes:");
        for (Map.Entry<String, AtomicInteger> entry : sentToNode.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue().get());
        }

        System.out.println("\nTotal local events generated: " + totalEvents.get());
        System.out.println("Total remote send percent configured: " + remoteSendPercent + "%");
        System.out.println("Actual send percentage: " + 
            String.format("%.2f", (sentEvents.get() * 100.0 / totalEvents.get())) + "%");
	}

	/**
	 * Main entry point for the NodeS application.
	 * Parses command line arguments and starts the node.
	 *
	 * @param args	command line arguments: nodeName 
	 * 										nodeIP 
	 * 										numThreads 
	 * 										eventsPerThread 
	 * 										remoteSendPercent
	 */
	public static void main(String[] args) {
		if (args.length != 5) {
			printUsage();
			System.exit(1);
		}

		try {
			String nodeName = args[0];
			String nodeIP = args[1];
			int numThreads = Integer.parseInt(args[2]);
			int eventsPerThread = Integer.parseInt(args[3]);
			int remoteSendPercent = Integer.parseInt(args[4]);

			// validate params
			if (!validateParameters(numThreads, eventsPerThread, remoteSendPercent)) {
				System.exit(1);
			}

			NodeS node = new NodeS(nodeName, nodeIP, numThreads, eventsPerThread, remoteSendPercent);
			node.start();

		} catch (NumberFormatException ex) {
			System.err.println("Error: Invalid number format in arguments");
			printUsage();
			System.exit(1);
		}
	}

	/**
     * Prints the correct usage of the NodeS command.
     */
    private static void printUsage() {
        System.err.println("Usage: java NodeS <nodeName> <nodeIP> <numThreads> <eventsPerThread> <remoteSendPercent>");
        System.err.println("Example: java NodeS AnaStanescu 160.10.23.18 200 500 25");
        System.err.println("  nodeName: name of this node (must match entry in nodes.csv)");
        System.err.println("  nodeIP: IP address of this node");
        System.err.println("  numThreads: number of worker threads (> 0)");
        System.err.println("  eventsPerThread: number of events per worker thread (> 0)");
        System.err.println("  remoteSendPercent: percentage (0-100) of events to send remotely");
    }

	/**
     * Validates the numeric parameters.
     * 
     * @param numThreads 		number of threads
     * @param eventsPerThread 	events per thread
     * @param remoteSendPercent	remote send percentage
	 *
     * @return true if all parameters are valid, 
	 * 		   false otherwise
     */
    private static boolean validateParameters(int numThreads, int eventsPerThread, int remoteSendPercent) {
        boolean valid = true;
        
        if (numThreads <= 0) {
            System.err.println("Error: numThreads must be greater than 0");
            valid = false;
        }
        
        if (eventsPerThread <= 0) {
            System.err.println("Error: eventsPerThread must be greater than 0");
            valid = false;
        }
        
        if (remoteSendPercent < 0 || remoteSendPercent > 100) {
            System.err.println("Error: remoteSendPercent must be between 0 and 100");
            valid = false;
        }
        
        return valid;
    }
}
