package com.distributed;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final int NUM_PROCESSES = 5;
    private static final int BASE_PORT = 10000;
    private static final int[] vectorClock = new int[NUM_PROCESSES];
    private static final Map<Integer, List<String>> results = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);

        // Step 1: Get input paragraph from the user
        System.out.println("Enter a paragraph:");
        String paragraph = scanner.nextLine();
        String[] words = paragraph.split(" ");

        // Step 2: Distribute words to workers randomly
        Map<Integer, List<String>> wordDistribution = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < NUM_PROCESSES; i++) {
            wordDistribution.put(i, new ArrayList<>());
        }
        for (String word : words) {
            int workerId = random.nextInt(NUM_PROCESSES);
            wordDistribution.get(workerId).add(word);
            incrementClock(0); // Update main process clock
        }

        // Step 3: Start threads to listen for worker responses
        ExecutorService executor = Executors.newFixedThreadPool(NUM_PROCESSES);
        for (int i = 0; i < NUM_PROCESSES; i++) {
            int workerId = i;
            executor.submit(() -> {
                try {
                    listenForWorkerResponse(workerId);
                } catch (IOException e) {
                    System.err.println("Error listening for Worker " + workerId + ": " + e.getMessage());
                }
            });
        }

        // Step 4: Send words to workers
        for (int i = 0; i < NUM_PROCESSES; i++) {
            if (!wordDistribution.get(i).isEmpty()) {
                sendMessageToWorker(i, wordDistribution.get(i));
            }
        }

        // Step 5: Wait for all workers to respond
        executor.shutdown();
        if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
            System.err.println("Timeout: Not all workers responded.");
        }

        // Step 6: Collect and print the processed paragraph
        List<String> collectedWords = new ArrayList<>();
        for (List<String> workerResult : results.values()) {
            collectedWords.addAll(workerResult);
        }
        System.out.println("Processed Paragraph: " + String.join(" ", collectedWords));
    }

    private static void sendMessageToWorker(int workerId, List<String> words) throws IOException {
        int port = BASE_PORT + workerId;
        try (Socket socket = new Socket("localhost", port);
             OutputStream out = socket.getOutputStream()) {

            Message message = Message.newBuilder()
                    .addAllWords(words)
                    .setSenderId(0)
                    .addAllVectorClock(toList(vectorClock))
                    .build();

            message.writeTo(out);
        }
    }

    private static void listenForWorkerResponse(int workerId) throws IOException {
        int port = BASE_PORT + workerId + 1000;
        try (ServerSocket serverSocket = new ServerSocket(port);
             Socket socket = serverSocket.accept();
             InputStream in = socket.getInputStream()) {

            Response response = Response.parseFrom(in);
            updateClock(response.getVectorClockList());
            results.put(workerId, response.getProcessedWordsList());
        }
    }

    private static void incrementClock(int processId) {
        vectorClock[processId]++;
    }

    private static void updateClock(List<Integer> receivedClock) {
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock.get(i));
        }
    }

    private static List<Integer> toList(int[] array) {
        List<Integer> list = new ArrayList<>();
        for (int value : array) {
            list.add(value);
        }
        return list;
    }
}
