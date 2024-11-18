package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import com.google.protobuf.InvalidProtocolBufferException;

public class Worker {
    private static final int WORKER_ID = 0; // Set unique ID for each worker
    private static final int BASE_PORT = 10000;
    private static final int[] vectorClock = new int[5];

    public static void main(String[] args) throws IOException {
        int port = BASE_PORT + WORKER_ID;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Worker " + WORKER_ID + " is running on port " + port);

        while (true) {
            try (Socket socket = serverSocket.accept();
                 InputStream in = socket.getInputStream()) {

                // Deserialize the message
                Message message = Message.parseFrom(in);

                // Update vector clock
                updateClock(message.getVectorClockList());
                vectorClock[WORKER_ID]++;

                // Process words (convert to uppercase)
                List<String> processedWords = new ArrayList<>();
                for (String word : message.getWordsList()) {
                    processedWords.add(word.toUpperCase());
                }

                // Send the response back
                sendResponse(processedWords);
            }
        }
    }

    private static void sendResponse(List<String> processedWords) throws IOException {
        int port = BASE_PORT + WORKER_ID + 1000; // Return port
        try (Socket socket = new Socket("localhost", port);
             OutputStream out = socket.getOutputStream()) {

            // Create Response using ProtoBuf
            Response response = Response.newBuilder()
                    .addAllProcessedWords(processedWords)
                    .addAllVectorClock(toList(vectorClock))
                    .build();

            // Serialize and send the response
            response.writeTo(out);
        }
    }

    private static void updateClock(List<Integer> receivedClock) {
        System.out.println(receivedClock.size());
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
