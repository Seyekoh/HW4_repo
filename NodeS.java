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
 *
 * @author James Bridges, Brailey Sharpe
 * @version Spring 2026
 */
public class NodeS {
    // Command-line parameters
    private final String nodeName;
    private final int numThreads;
    private final int eventsPerThread;
    private final int remoteSendPercent;

    // Core components
    private final LamportClock clock;
    private final List<NodeInfo> remoteNodes;
    private final Random random;
    private final int port;

    // Metrics
    private final AtomicLong localEventsGenerated;
    private final AtomicLong totalProcessedEvents;
    private final AtomicInteger sentEvents;
    private final AtomicInteger receivedEvents;
    private final AtomicInteger eventsLogged;
    private final Map<String, AtomicInteger> sentToNode;

    // Thread management
    private final List<Thread> workerThreads;
    private final List<Thread> senderThreads;
    private final List<Thread> handlerThreads;
    private final CountDownLatch workersLatch;
    private final AtomicInteger activeSenders;
    private final AtomicInteger activeHandlers;
    private final Object completionLock;
    private final AtomicBoolean shutdownInitiated;
    private volatile boolean running;
    private volatile long startTime;

    // Server components
    private ServerSocket serverSocket;
    private Thread serverThread;
    private Thread shutdownListenerThread;

    // Logging
    private PrintWriter logWriter;

    /**
     * Constructor for NodeS
     * Initializes all components and loads remote nodes from nodes.csv.
     *
     * @param nodeName          the name of this node
     * @param nodeIP            the IP address of this node
     * @param numThreads        number of worker threads to create
     * @param eventsPerThread   number of events each worker thread will process
     * @param remoteSendPercent percentave chanve to send event to remote node
     */
    public NodeS(String nodeName, String nodeIP, int numThreads, int eventsPerThread, int remoteSendPercent) {
        this.nodeName = nodeName;
        this.numThreads = numThreads;
        this.eventsPerThread = eventsPerThread;
        this.remoteSendPercent = remoteSendPercent;

        // Initialize logging
        initLogging();

        // Initialize core components
        this.port = 4225;
        this.random = new Random(4225);
        int lamportIncrement = getLamportIncrement(nodeIP);
        this.clock = new LamportClock(lamportIncrement);
        this.remoteNodes = new ArrayList<>();
        this.workersLatch = new CountDownLatch(numThreads);

        // Initialize metrics
        this.localEventsGenerated = new AtomicLong(0);
        this.totalProcessedEvents = new AtomicLong(0);
        this.sentEvents = new AtomicInteger(0);
        this.receivedEvents = new AtomicInteger(0);
        this.eventsLogged = new AtomicInteger(0);
        this.sentToNode = new ConcurrentHashMap<>();

        // Initialize thread management
        this.workerThreads = Collections.synchronizedList(new ArrayList<>());
        this.senderThreads = Collections.synchronizedList(new ArrayList<>());
        this.handlerThreads = Collections.synchronizedList(new ArrayList<>());
        this.activeSenders = new AtomicInteger(0);
        this.activeHandlers = new AtomicInteger(0);
        this.completionLock = new Object();
        this.shutdownInitiated = new AtomicBoolean(false);
        this.running = true;

        // Load remote nodes from CSV
        loadRemoteNodes();

        log("Node " + nodeName + " initialized on IP " + nodeIP);
        log("Configuration: threads = " + numThreads +
                ", eventsPerThread = " + eventsPerThread +
                ", remoteSendPercent = " + remoteSendPercent + "%");
    }

    /**
     * Initialize logging to a file named <nodeName>.log
     */
    private void initLogging() {
        try {
            String logFileName = nodeName + "_events.log";
            logWriter = new PrintWriter(new FileWriter(logFileName), true);
            System.out.println("Logging to: " + logFileName);
        } catch (IOException ex) {
            System.err.println("Failed to create log file: " + ex.getMessage());

            // Fallback to console only
            logWriter = null;
        }
    }

    /**
     * Log a message to both console and log file
     *
     * @param message the message to log
     */
    private void log(String message) {
        System.out.println(message);
        if (logWriter != null) {
            logWriter.println(message);
        }
    }

    /**
     * Extracts the Lamport increment value from an IP address.
     * Uses the last non-zero digit of the last octet.
     *
     * @param ip the IP adress (e.g., "160.10.23.18"
     * @return the increment value (1-9)
     */
    private int getLamportIncrement(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length == 4) {
            String lastOctet = parts[3];

            // Find the last non-zero digit
            for (int i = lastOctet.length() - 1; i >= 0; i--) {
                char c = lastOctet.charAt(i);
                if (c != '0') {
                    return Character.getNumericValue(c);
                }
            }
        }

        return 1; // default to 1 if no non-zero digit found
    }

    /**
     * Main entry point for the node.
     * Starts the server, workers, and shutdown listener, then waits for completion.
     */
    public void start() {
        try {
            startServer();
            startShutdownListener();

            Thread.sleep(1000);

            startTime = System.currentTimeMillis();
            startWorkerThreads();

            workersLatch.await();

            joinAllThreads(senderThreads);
            waitForCommunicationToFinish();

            if (shutdownInitiated.get()) {
                return;
            }

            running = false;
            closeServerSocket();

            joinAllThreads(handlerThreads);

            if (serverThread != null) {
                serverThread.join();
            }

            if (shutdownInitiated.get()) {
                return;
            }

            long endTime = System.currentTimeMillis();
            printStatistics(endTime - startTime);

            if (logWriter != null) {
                logWriter.close();
            }

        } catch (InterruptedException ex) {
            log("Node " + nodeName + " interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log("Node " + nodeName + " error: " + ex.getMessage());
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
            log("Warning: nodes.csv not found in current directory");
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
                    log("Warning: Invalid format at line " + lineNum + ": " + line);
                    continue;
                }

                String name = parts[0].trim();
                String ip = parts[1].trim();

                if (!name.equals(nodeName)) {
                    remoteNodes.add(new NodeInfo(name, ip));
                    sentToNode.put(name, new AtomicInteger(0));
                }
            }
            log("Loaded " + remoteNodes.size() + " remote nodes");

        } catch (IOException ex) {
            log("Error loading nodes.csv: " + ex.getMessage());
        }
    }

    /**
     * Starts the server thread that listens for incoming event connections.
     * Each incoming connection is handled in a separate thread.
     */
    private void startServer() {
        serverThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(port, 5000);
                log("Node " + nodeName + " listening on port " + port);

                while (running) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        Thread handlerThread = new Thread(() -> handleClient(clientSocket));
                        handlerThreads.add(handlerThread);
                        handlerThread.start();
                    } catch (SocketException ex) {
                        if (running) {
                            log("Server accept error: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                if (running) {
                    log("Failed to start server: " + ex.getMessage());
                }
            } finally {
                closeServerSocket();
            }
        });
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
     * @param socket the client socket connected to this node
     */
    private void handleClient(Socket socket) {
        handlerStarted();
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
            Event event = (Event) ois.readObject();

            clock.update(event.getTimestamp());
            log("RECEIVED: " + event);
            eventsLogged.incrementAndGet();

            receivedEvents.incrementAndGet();
            totalProcessedEvents.incrementAndGet();

        } catch (IOException | ClassNotFoundException ex) {
            log("Error handling client: " + ex.getMessage());
            eventsLogged.incrementAndGet();
        } finally {
            closeSocket(socket);
            handlerFinished();
        }
    }

    /**
     * Closes a socket safely.
     *
     * @param socket the socket to close
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

    private void senderStarted() {
        activeSenders.incrementAndGet();
    }

    private void senderFinished() {
        if (activeSenders.decrementAndGet() == 0) {
            synchronized (completionLock) {
                completionLock.notifyAll();
            }
        }
    }

    private void handlerStarted() {
        activeHandlers.incrementAndGet();
    }

    private void handlerFinished() {
        if (activeHandlers.decrementAndGet() == 0) {
            synchronized (completionLock) {
                completionLock.notifyAll();
            }
        }
    }

    private void waitForCommunicationToFinish() throws InterruptedException {
        synchronized (completionLock) {
            while (activeSenders.get() > 0 || activeHandlers.get() > 0) {
                completionLock.wait();
            }
        }
    }

    private void joinAllThreads(List<Thread> threads) throws InterruptedException {
        List<Thread> snapshot;
        synchronized (threads) {
            snapshot = new ArrayList<>(threads);
        }

        for (Thread thread : snapshot) {
            if (thread != null) {
                thread.join();
            }
        }
    }

    /**
     * Starts the worker threads that generate and process events.
     * Each worker thread runs independently and processes eventsPerThread events.
     */
    private void startWorkerThreads() {
        log("Starting " + numThreads + " worker threads with " + eventsPerThread + " events each");

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
     * @param threadId the Id fo this worker thread (0-based)
     */
    private void processWorkerEvents(int threadId) {
        try {
            for (int eventNum = 0; eventNum < eventsPerThread && running; eventNum++) {
                long timestamp = clock.tick();

                if (shouldSendToRemote() && !remoteNodes.isEmpty()) {
                    sendToRandomRemote(threadId, timestamp);
                } else {
                    processLocally(threadId, timestamp);
                }

                localEventsGenerated.incrementAndGet();
                totalProcessedEvents.incrementAndGet();
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
     * false otherwise
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
     * @param threadId  the ID of the worker thread sending the event
     * @param timestamp the current Lamport timestamp
     */
    private void sendToRandomRemote(int threadId, long timestamp) {
        NodeInfo target = remoteNodes.get(random.nextInt(remoteNodes.size()));
        Event event = new Event(nodeName, target.name, timestamp);

        sentEvents.incrementAndGet();
        sentToNode.get(target.name).incrementAndGet();
        log("Thread-" + threadId + " SENT: " + event);
        eventsLogged.incrementAndGet();

        sendEvent(target, event);
    }

    /**
     * Processes an event locally.
     *
     * @param threadId  The ID of the worker thread processing the event
     * @param timestamp the current Lamport timestamp
     */
    private void processLocally(int threadId, long timestamp) {
        Event event = new Event(nodeName, nodeName, timestamp);
        log("Thread-" + threadId + " LOCAL: " + event);
        eventsLogged.incrementAndGet();
    }

    /**
     * Sends an event to a remote node over the network.
     * Uses a seperate thread to avoid blocking the worker.
     *
     * @param target the target node information
     * @param event  the event to send
     */
    private void sendEvent(NodeInfo target, Event event) {
        Thread sender = new Thread(() -> {
	    int attempts = 0;
	    int maxAttempts = 5;
	    boolean success = false;
	    
	    while (!success && attempts < maxAttempts) {
	        try (Socket socket = new Socket(target.ip, port);
		     ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
		    oos.writeObject(event);
		    oos.flush();
		    success = true;
		    sentEvents.incrementAndGet();
	        } catch (IOException ex) {
		    attempts++;
		    if (attempts < maxAttempts) {
			try {
			    Thread.sleep(50);
			} catch (InterruptedException ie) {
			    Thread.currentThread().interrupt();
			}
		    } else {
			log("Failed to send to " + target.name + " after " + maxAttempts + " tries.");
		    }
		}
	    }	
	});
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
        if (!shutdownInitiated.compareAndSet(false, true)) {
            return;
        }

        log("\nShutdown command received. Stopping...");
        running = false;

        try {
            workersLatch.await();
            log("All worker threads completed.");

            joinAllThreads(senderThreads);
            waitForCommunicationToFinish();

            closeServerSocket();

            joinAllThreads(handlerThreads);

            if (serverThread != null) {
                serverThread.join();
            }

            long endTime = System.currentTimeMillis();
            printStatistics(endTime - startTime);
            log("Node " + nodeName + " shutdown complete.");

            if (logWriter != null) {
                logWriter.close();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            System.exit(0);
        }
    }

    /**
     * Prints final statistics for this node.
     * Includes execution time, event counts, throughput, and Lamport timestamp.
     *
     * @param executionTime the total execution time im milliseconds
     */
    private void printStatistics(long executionTime) {
        double throughput = executionTime > 0
                ? totalProcessedEvents.get() / (executionTime / 1000.0)
                : 0.0;

        log("\n=== Node " + nodeName + " Statistics ===");
        log("Total local events generated: " + localEventsGenerated.get());
        log("Total events processed: " + totalProcessedEvents.get());
        log("Messages sent: " + sentEvents.get());
        log("Messages received: " + receivedEvents.get());
        log("Total events logged: " + eventsLogged.get());
        log("Execution time = " + executionTime + " ms");
        log("Final Lamport time: " + clock.getTime());
        log("Throughput: " + String.format("%.2f", throughput) + " events/sec");

        log("\nEvents sent to remote nodes:");
        for (Map.Entry<String, AtomicInteger> entry : sentToNode.entrySet()) {
            log("  " + entry.getKey() + ": " + entry.getValue().get());
        }

        log("Total remote send percent configured: " + remoteSendPercent + "%");
        log("Actual send percentage: " +
                String.format("%.2f",
                        localEventsGenerated.get() > 0
                                ? (sentEvents.get() * 100.0 / localEventsGenerated.get())
                                : 0.0) + "%");
    }

    /**
     * Main entry point for the NodeS application.
     * Parses command line arguments and starts the node.
     *
     * @param args command line arguments: nodeName
     *             nodeIP
     *             numThreads
     *             eventsPerThread
     *             remoteSendPercent
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
     * @param numThreads        number of threads
     * @param eventsPerThread   events per thread
     * @param remoteSendPercent remote send percentage
     * @return true if all parameters are valid,
     * false otherwise
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
