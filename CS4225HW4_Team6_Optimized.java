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
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class CS4225HW4_Team6_Optimized {
    private static final long BASE_RANDOM_SEED = 4225L;
    private static final int PORT = 4225;
    private static final String LOG_FILE_NAME = "events.log";
    private static final String LOG_POISON = "__NODE_O_LOG_POISON__";
    private static final long LOGGER_FLUSH_BATCH = 256L;
    private static final int CONNECT_TIMEOUT_MS = 1500;
    private static final int SEND_RETRIES = 10;
    private static final long STARTUP_DELAY_MS = 2000L;

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
    private final LongAdder sendAttempts;
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

    public CS4225HW4_Team6_Optimized(String nodeName, String nodeIP, int numThreads, int eventsPerThread, int remoteSendPercent) {
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
        this.sendAttempts = new LongAdder();
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

            Thread.sleep(STARTUP_DELAY_MS);

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
            log("Warning: nodes.csv not found");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 2) {
                    continue;
                }

                String name = parts[0].trim();
                String ip = parts[1].trim();

                if (!name.equals(nodeName)) {
                    remoteNodes.add(new NodeInfo(name, ip));
                    sentToNode.put(name, new LongAdder());
                }
            }
        } catch (IOException ex) {
            log("Error loading nodes.csv: " + ex.getMessage());
        }
    }

    private void startServer() {
        serverThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(PORT, 5000);

                while (running.get()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        if (clientSocket != null) {
                            handlerPool.submit(() -> handleClient(clientSocket));
                        }
                    } catch (SocketException ex) {
                        if (running.get()) {
                            log("Server error: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                if (running.get()) {
                    log("Server start error: " + ex.getMessage());
                }
            }
        });

        serverThread.start();
    }

    private void handleClient(Socket socket) {
        try (Socket client = socket;
             ObjectInputStream ois = new ObjectInputStream(client.getInputStream())) {

            Event event = (Event) ois.readObject();
            clock.update(event.getTimestamp());

            receivedEvents.increment();
            totalProcessedEvents.increment();

            logEvent("RECEIVED: " + event);
        } catch (Exception ex) {
            if (running.get()) {
                logEvent("Receive error: " + ex.getMessage());
            }
        }
    }

    private void startWorkerTasks() {
        for (int i = 0; i < numThreads; i++) {
            final int workerId = i;
            workerPool.submit(() -> processWorkerEvents(workerId));
        }
    }

    private void processWorkerEvents(int workerId) {
        Random workerRandom = new Random(BASE_RANDOM_SEED + workerId);

        try {
            for (int i = 0; i < eventsPerThread && running.get(); i++) {
                long timestamp = clock.tick();

                if (!remoteNodes.isEmpty() && shouldSendToRemote(workerRandom)) {
                    sendAttempts.increment();
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

    private boolean shouldSendToRemote(Random random) {
        return random.nextInt(100) < remoteSendPercent;
    }

    private void sendToRandomRemote(int workerId, long timestamp, Random random) {
        NodeInfo target = remoteNodes.get(random.nextInt(remoteNodes.size()));
        Event event = new Event(nodeName, target.name, timestamp);

        logEvent("Worker-" + workerId + " QUEUED FOR SEND: " + event);
        sendPool.submit(() -> sendEvent(workerId, target, event));
    }

    private void processLocally(int workerId, long timestamp) {
        Event event = new Event(nodeName, nodeName, timestamp);
        logEvent("Worker-" + workerId + " LOCAL: " + event);
    }

    private void sendEvent(int workerId, NodeInfo target, Event event) {
        for (int attempt = 1; attempt <= SEND_RETRIES && running.get(); attempt++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(target.ip, PORT), CONNECT_TIMEOUT_MS);

                try (ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
                    oos.writeObject(event);
                    oos.flush();
                }

                sentEvents.increment();
                sentToNode.get(target.name).increment();
                logEvent("Worker-" + workerId + " SENT: " + event);
                return;
            } catch (IOException ex) {
                if (attempt == SEND_RETRIES) {
                    logEvent("FAILED SEND: " + event);
                }
            }
        }
    }

    private void startShutdownListener() {
        shutdownListenerThread = new Thread(() -> {
            try (Scanner scanner = new Scanner(System.in)) {
                while (running.get()) {
                    if (scanner.nextLine().trim().equalsIgnoreCase("q")) {
                        finishRun(true);
                        break;
                    }
                }
            }
        });
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

            handlerPool.shutdown();
            handlerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            long endTime = System.currentTimeMillis();
            printStatistics(endTime - startTime);

            logQueue.offer(LOG_POISON);

            loggerThread.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void printStatistics(long executionTime) {
        long totalLocal = localEventsGenerated.sum();
        long totalSent = sentEvents.sum();
        long totalAttempts = sendAttempts.sum();

        log("Configured: " + remoteSendPercent + "%");
        log("Attempted: " + (totalAttempts * 100.0 / totalLocal) + "%");
        log("Successful: " + (totalSent * 100.0 / totalLocal) + "%");
    }

    public static void main(String[] args) {
        String nodeName = args[0];
        String nodeIP = args[1];
        int numThreads = Integer.parseInt(args[2]);
        int eventsPerThread = Integer.parseInt(args[3]);
        int remoteSendPercent = Integer.parseInt(args[4]);

        new CS4225HW4_Team6_Optimized(
                nodeName, nodeIP, numThreads, eventsPerThread, remoteSendPercent
        ).start();
    }
}
