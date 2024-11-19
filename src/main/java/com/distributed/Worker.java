package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;

public class Worker {
    private final int workerId;
    private final int[] vectorClock;

    public Worker(int workerId, int numProcesses) {
        this.workerId = workerId;
        this.vectorClock = new int[numProcesses + 1];
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(10000 + workerId)) {
            System.out.println("Worker " + workerId + " started on port " + (10000 + workerId));
            while (true) {
                try (Socket socket = serverSocket.accept();
                     InputStream in = socket.getInputStream()) {

                    // Receives tasks from the main process
                    Message message = Message.parseFrom(in);

                    // update Vector Clock
                    updateClock(message.getVectorClockList());

                    // handle task
                    List<Word> processedWords = processWords(message.getWordsList());

                    // add Vector Clock
                    incrementClock(workerId);

                    // handle response data
                    try (Socket responseSocket = new Socket("localhost", 10000 + workerId + 1000);
                         OutputStream out = responseSocket.getOutputStream()) {
                        Response response = Response.newBuilder()
                                .addAllProcessedWords(processedWords)
                                .addAllVectorClock(toList(vectorClock))
                                .build();
                        response.writeTo(out);
                    }
                }
            }
        }
    }

    private List<Word> processWords(List<Word> words) {
        List<Word> processedWords = new ArrayList<>();
        for (Word word : words) {
            processedWords.add(Word.newBuilder()
                    .setText(word.getText().toUpperCase()) // change to upper case
                    .setOriginalIndex(word.getOriginalIndex())
                    .build());
        }
        return processedWords;
    }

    private void updateClock(List<Integer> receivedClock) {
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock.get(i));
        }
    }

    private void incrementClock(int processId) {
        vectorClock[processId]++;
    }

    private List<Integer> toList(int[] array) {
        List<Integer> list = new ArrayList<>();
        for (int value : array) {
            list.add(value);
        }
        return list;
    }

    public static void main(String[] args) {
        int numProcess = 5;
        int workerId = 4;
        Worker worker = new Worker(workerId, numProcess); //
        try {
            worker.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}