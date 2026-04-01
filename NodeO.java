import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Optimized version (O) of the distributed node.
 *
 * Command line: java NodeO <nodeName> <nodeIP> <numThreads> <eventsPerThread> <remoteSendPercent>
 *
 * @author James Bridges, Brailey Sharpe
 * @version Spring 2026
 */
public class NodeO {
    private static final long BASE_RANDOM_SEED = 4225L;
    private static final int PORT = 4225;
    private static final String LOG_FILE_NAME = "events.log";
    private static final String LOG_POISON = "__NODE_O_LOG_POISON__";
    private static final long LOGGER_FLUSH_BATCH = 256L;
    private static final int CONNECT_TIMEOUT_MS = 1500;
    private static final int SEND_RETRIES = 3;

    private final String nodeName;
    private final int numThreads;
    private final int eventsPerThread;
    private final int remoteSendPercent;

    private final LamportClock clock;
    private final List<NodeInfo> remoteNodes;

    private final LongAdder localEventsGenerated;
    private final LongAdder totalProcessedEvents;
    private final LongAdder sentEvents;
    private final LongAdder receivedEvents;
    private final LongAdder eventsLogged;
    private final Map<String, LongAdder> sentToNode;

    private final CountDownLatch workersLatch;
    private final AtomicBoolean running;
    private final AtomicBoolean finalizationStarted;

    private final ExecutorService workerPool;
    private final ExecutorService sendPool;
    private final ExecutorService handlerPool;

    private final LinkedBlockingQueue<String> logQueue;

    private volatile long startTime;
    private volatile ServerSocket serverSocket;
    private volatile Thread serverThread;
    private volatile Thread shutdownListenerThread;
    private volatile Thread loggerThread;
    private volatile BufferedWriter logWriter;

    public NodeO(String nodeName, String nodeIP, int numThreads, int eventsPerThread, int remoteSendPercent) {
        this.nodeName = nodeName;
        this.numThreads = numThreads;
        this.eventsPerThread = eventsPerThread;
        this.remoteSendPercent = remoteSendPercent;

        int lamportIncrement = getLamportIncrement(nodeIP);
        this.clock = new LamportClock(lamportIncrement);
        this.remoteNodes = new ArrayList<>();

        this.localEventsGenerated = new LongAdder();
        this.totalProcessedEvents = new LongAdder();
        this.sentEvents = new LongAdder();
        this.receivedEvents = new LongAdder();
        this.eventsLogged = new LongAdder();
        this.sentToNode = new ConcurrentHashMap<>();

        this.workersLatch = new CountDownLatch(numThreads);
        this.running = new AtomicBoolean(true);
        this.finalizationStarted = new AtomicBoolean(false);

        this.workerPool = Executors.newFixedThreadPool(numThreads);
        this.sendPool = Executors.newFixedThreadPool(getSendPoolSize(numThreads));
        this.handlerPool = Executors.newFixedThreadPool(getHandlerPoolSize(numThreads));
        this.logQueue = new LinkedBlockingQueue<>();

        initLogging();
        loadRemoteNodes();

        log("Node " + nodeName + " initialized");
        log("Configuration: threads = " + numThreads
                + ", eventsPerThread = " + eventsPerThread
                + ", remoteSendPercent = " + remoteSendPercent + "%");
    }

    private static int getSendPoolSize(int numThreads) {
        int processors = Runtime.getRuntime().availableProcessors();
        return Math.max(4, Math.min(numThreads, processors * 2));
    }

    private static int getHandlerPoolSize(int numThreads) {
        int processors = Runtime.getRuntime().availableProcessors();
        return Math.max(4, Math.min(numThreads, processors * 2));
    }

    private void initLogging() {
        try {
            this.logWriter = new BufferedWriter(new FileWriter(LOG_FILE_NAME));
            this.loggerThread = new Thread(this::runLogger, "NodeO-Logger");
            this.loggerThread.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to create log file: " + ex.getMessage(), ex);
        }
    }

    private void runLogger() {
        long bufferedMessages = 0;

        try {
            while (true) {
                String message = logQueue.take();
                if (LOG_POISON.equals(message)) {
                    break;
                }

                System.out.println(message);
                logWriter.write(message);
                logWriter.newLine();

                bufferedMessages++;
                if (bufferedMessages >= LOGGER_FLUSH_BATCH) {
                    logWriter.flush();
                    bufferedMessages = 0;
                }
            }

            while (!logQueue.isEmpty()) {
                String message = logQueue.poll();
                if (message == null || LOG_POISON.equals(message)) {
                    continue;
                }

                System.out.println(message);
                logWriter.write(message);
                logWriter.newLine();
            }

            logWriter.flush();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (IOException ex) {
            System.err.println("Logger thread error: " + ex.getMessage());
        } finally {
            try {
                if (logWriter != null) {
                    logWriter.flush();
                    logWriter.close();
                }
            } catch (IOException ex) {
                System.err.println("Failed to close log file: " + ex.getMessage());
            }
        }
    }

    private void log(String message) {
        logQueue.offer(message);
    }

    private void logEvent(String message) {
        eventsLogged.increment();
        log(message);
    }

    private int getLamportIncrement(String ip) {
        String[] parts = ip.split("\\.");
        if (parts.length == 4) {
            String lastOctet = parts[3];
            for (int i = lastOctet.length() - 1; i >= 0; i--) {
                char c = lastOctet.charAt(i);
                if (c != '0') {
                    return Character.getNumericValue(c);
                }
            }
        }
        return 1;
    }

    public void start() {
        try {
            startServer();
            startShutdownListener();
            waitForRemoteNodes();

            startTime = System.currentTimeMillis();
            startWorkerTasks();

            workersLatch.await();
            workerPool.shutdown();
            workerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            finishRun(false);
        } catch (InterruptedException ex) {
            log("Node " + nodeName + " interrupted");
            Thread.currentThread().interrupt();
            finishRun(true);
        } catch (Exception ex) {
            log("Node " + nodeName + " error: " + ex.getMessage());
            finishRun(true);
        }
    }

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

                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split(",");
                if (parts.length != 2) {
                    log("Warning: Invalid format at line " + lineNum + ": " + line);
                    continue;
                }

                String name = parts[0].trim();
                String ip = parts[1].trim();

                if (!name.equals(nodeName)) {
                    remoteNodes.add(new NodeInfo(name, ip));
                    sentToNode.put(name, new LongAdder());
                }
            }

            log("Loaded " + remoteNodes.size() + " remote nodes");
        } catch (IOException ex) {
            log("Error loading nodes.csv: " + ex.getMessage());
        }
    }

    private void startServer() {
        serverThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(PORT, 5000);
                log("Node " + nodeName + " listening on port " + PORT);

                while (running.get()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        clientSocket.setTcpNoDelay(true);
                        handlerPool.submit(() -> handleClient(clientSocket));
                    } catch (SocketException ex) {
                        if (running.get()) {
                            log("Server accept error: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                if (running.get()) {
                    log("Failed to start server: " + ex.getMessage());
                }
            } finally {
                closeServerSocket();
            }
        }, "NodeO-Server");

        serverThread.start();
    }

    private void closeServerSocket() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ex) {
            log("Server socket close error: " + ex.getMessage());
        }
    }

    private void handleClient(Socket socket) {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            Event event = (Event) ois.readObject();
            clock.update(event.getTimestamp());

            receivedEvents.increment();
            totalProcessedEvents.increment();

            logEvent("RECEIVED: " + event);
        } catch (IOException ex) {
            logEvent("Error handling client: " + ex.getMessage());
        } catch (ClassNotFoundException ex) {
	    logEvent("Error finding class: " + ex.getMessage());
	} finally {
	    closeSocket(socket);
	}
    }

    private void waitForRemoteNodes() {
        if (remoteNodes.isEmpty()) {
            return;
        }

        boolean[] ready = new boolean[remoteNodes.size()];
        int readyCount = 0;

        log("Waiting for remote nodes to become reachable...");

        while (readyCount < remoteNodes.size() && running.get()) {
            for (int i = 0; i < remoteNodes.size(); i++) {
                if (ready[i]) {
                    continue;
                }

                NodeInfo node = remoteNodes.get(i);

                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.ip, PORT), 500);
                    ready[i] = true;
                    readyCount++;
                    log("Remote node reachable: " + node.name);
                } catch (IOException ex) {
                }
            }

            if (readyCount < remoteNodes.size()) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        log("All remote nodes are reachable. Starting workers.");
    }

    private void startWorkerTasks() {
        log("Starting " + numThreads + " worker tasks with " + eventsPerThread + " events each");

        for (int i = 0; i < numThreads; i++) {
            final int workerId = i;
            workerPool.submit(() -> processWorkerEvents(workerId));
        }
    }

    private void processWorkerEvents(int workerId) {
        SplittableRandom workerRandom = new SplittableRandom(BASE_RANDOM_SEED + 0x9E3779B97F4A7C15L * (workerId + 1L));

        try {
            for (int eventNum = 0; eventNum < eventsPerThread && running.get(); eventNum++) {
                long timestamp = clock.tick();

                if (!remoteNodes.isEmpty() && shouldSendToRemote(workerRandom)) {
                    sendToRandomRemote(workerId, timestamp, workerRandom);
                } else {
                    processLocally(workerId, timestamp);
                }

                localEventsGenerated.increment();
                totalProcessedEvents.increment();
            }
        } finally {
            workersLatch.countDown();
        }
    }

    private boolean shouldSendToRemote(SplittableRandom random) {
        return random.nextInt(100) < remoteSendPercent;
    }

    private void sendToRandomRemote(int workerId, long timestamp, SplittableRandom random) {
        NodeInfo target = remoteNodes.get(random.nextInt(remoteNodes.size()));
        Event event = new Event(nodeName, target.name, timestamp);

        sentEvents.increment();
        sentToNode.get(target.name).increment();

        logEvent("Worker-" + workerId + " SENT: " + event);

        sendPool.submit(() -> sendEvent(target, event));
    }

    private void processLocally(int workerId, long timestamp) {
        Event event = new Event(nodeName, nodeName, timestamp);
        logEvent("Worker-" + workerId + " LOCAL: " + event);
    }

    private void sendEvent(NodeInfo target, Event event) {
        for (int attempt = 1; attempt <= SEND_RETRIES; attempt++) {
            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(target.ip, PORT), CONNECT_TIMEOUT_MS);

                try (ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
                    oos.writeObject(event);
                    oos.flush();
                }

                return;
            } catch (IOException ex) {
                if (attempt == SEND_RETRIES) {
                    logEvent("Failed to send event to " + target.name + ": " + ex.getMessage());
                } else {
                    try {
                        Thread.sleep(50L * attempt);
                    } catch (InterruptedException interruptedEx) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private void startShutdownListener() {
        shutdownListenerThread = new Thread(() -> {
            try (Scanner scanner = new Scanner(System.in)) {
                while (running.get()) {
                    String input = scanner.nextLine().trim();
                    if (input.equalsIgnoreCase("q")) {
                        finishRun(true);
                        break;
                    }
                }
            } catch (Exception ex) {
                if (running.get()) {
                    log("Shutdown listener error: " + ex.getMessage());
                }
            }
        }, "NodeO-ShutdownListener");

        shutdownListenerThread.setDaemon(true);
        shutdownListenerThread.start();
    }

    private void finishRun(boolean shutdownRequested) {
        if (!finalizationStarted.compareAndSet(false, true)) {
            return;
        }

        running.set(false);

        try {
            workersLatch.await();

            workerPool.shutdown();
            workerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            sendPool.shutdown();
            sendPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            closeServerSocket();

            if (serverThread != null) {
                serverThread.join();
            }

            handlerPool.shutdown();
            handlerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            long endTime = System.currentTimeMillis();
            printStatistics(endTime - startTime);

            logQueue.offer(LOG_POISON);

            if (loggerThread != null) {
                loggerThread.join();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            if (shutdownRequested) {
                System.exit(0);
            }
        }
    }

    private void printStatistics(long executionTime) {
        long totalProcessed = totalProcessedEvents.sum();
        long totalLocal = localEventsGenerated.sum();
        long totalSent = sentEvents.sum();
        long totalReceived = receivedEvents.sum();
        long totalLogged = eventsLogged.sum();

        double throughput = executionTime > 0
                ? totalProcessed / (executionTime / 1000.0)
                : 0.0;

        log("=== Node " + nodeName + " Statistics ===");
        log("Total local events generated: " + totalLocal);
        log("Total events processed: " + totalProcessed);
        log("Messages sent: " + totalSent);
        log("Messages received: " + totalReceived);
        log("Total events logged: " + totalLogged);
        log("Execution time = " + executionTime + " ms");
        log("Final Lamport time: " + clock.getTime());
        log("Throughput: " + String.format("%.2f", throughput) + " events/sec");
        log("Events sent to remote nodes:");

        for (Map.Entry<String, LongAdder> entry : sentToNode.entrySet()) {
            log("  " + entry.getKey() + ": " + entry.getValue().sum());
        }

        log("Total remote send percent configured: " + remoteSendPercent + "%");
        log("Actual send percentage: " + String.format("%.2f",
                totalLocal > 0 ? (totalSent * 100.0 / totalLocal) : 0.0) + "%");
    }

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

            if (!validateParameters(numThreads, eventsPerThread, remoteSendPercent)) {
                System.exit(1);
            }

            NodeO node = new NodeO(nodeName, nodeIP, numThreads, eventsPerThread, remoteSendPercent);
            node.start();
        } catch (NumberFormatException ex) {
            System.err.println("Error: Invalid number format in arguments");
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("Usage: java NodeO <nodeName> <nodeIP> <numThreads> <eventsPerThread> <remoteSendPercent>");
        System.err.println("Example: java NodeO AnaStanescu 160.10.23.18 200 500 25");
        System.err.println("  nodeName: name of this node (must match entry in nodes.csv)");
        System.err.println("  nodeIP: IP address of this node");
        System.err.println("  numThreads: number of worker tasks (> 0)");
        System.err.println("  eventsPerThread: number of events per worker task (> 0)");
        System.err.println("  remoteSendPercent: percentage (0-100) of events to send remotely");
    }

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
