package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;


public class Main {
    private static final int NUM_PROCESSES = 5;
    private static final int BASE_PORT = 5000;
    private static final int[] vectorClock = new int[NUM_PROCESSES];

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

        // Step 3: Send words to workers
        for (int i = 0; i < NUM_PROCESSES; i++) {
            if (!wordDistribution.get(i).isEmpty()) {
                sendMessageToWorker(i, wordDistribution.get(i));
            }
        }

        // Step 4: Wait for workers to process
        System.out.println("Waiting for workers to process...");
        Thread.sleep(15000);

        // Step 5: Collect responses from workers
        List<String> collectedWords = new ArrayList<>();
        for (int i = 0; i < NUM_PROCESSES; i++) {
            collectedWords.addAll(receiveResponseFromWorker(i));
        }

        // Step 6: Print the reconstructed paragraph
        System.out.println("Processed Paragraph: " + String.join(" ", collectedWords));
    }

    private static void sendMessageToWorker(int workerId, List<String> words) throws IOException {
        int port = BASE_PORT + workerId;
        Socket socket = new Socket("localhost", port);
        try (OutputStream out = socket.getOutputStream()) {
            // Create Message using ProtoBuf
            Message message = Message.newBuilder()
                    .addAllWords(words)
                    .setSenderId(0) // Main process ID
                    .addAllVectorClock(toList(vectorClock))
                    .build();

            // Serialize and send the message
            message.writeTo(out);
        }
    }

    private static List<String> receiveResponseFromWorker(int workerId) throws IOException {
        int port = BASE_PORT + workerId + 1000; // Return port
        ServerSocket serverSocket = new ServerSocket(port);
        try (Socket socket = serverSocket.accept();
             InputStream in = socket.getInputStream()) {

            // Deserialize the response using ProtoBuf
            Response response = Response.parseFrom(in);
            updateClock(response.getVectorClockList());
            return response.getProcessedWordsList();
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
