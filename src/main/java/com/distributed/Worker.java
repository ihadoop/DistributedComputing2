package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.*;

public class Worker {
    private final int workerId;
    private final int basePort;
    private final int[] vectorClock;

    public Worker(int workerId, int numWorkers) {
        this.workerId = workerId;
        this.basePort = 10000 + workerId;
        this.vectorClock = new int[numWorkers + 1];
    }

    public static void main(String[] args) {

        int numWorkers = 5;


        int workerId = 1;
        new Worker(workerId, numWorkers).start();


        /**
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i;
            new Thread(()->{
                new Worker(workerId, numWorkers).start();
            }).start();

        }
         */

    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(basePort)) {
            System.out.println("Worker " + workerId + " started on port " + basePort);

            while (true) {
                try (Socket socket = serverSocket.accept();
                     InputStream in = socket.getInputStream();
                     OutputStream out = socket.getOutputStream()) {

                    // receive task
                    Message message = Message.parseFrom(in);
                    System.out.println("WorkId "+workerId+",Received message from main: " + message.getWordsList());
                    // update Vector Clock
                    updateClock(message.getVectorClockList());
                    incrementClock(workerId);

                    // handler task
                    List<Word> processedWords = new ArrayList<>();
                    for (Word word : message.getWordsList()) {
                        String processedText = word.getText().toUpperCase(); // 模拟处理逻辑
                        processedWords.add(word.toBuilder().setText(processedText).build());
                    }

                    // wait Main
                    ServerSocket resultServer = new ServerSocket(basePort + 1000);
                    Socket resultSocket = resultServer.accept();

                    // send result data
                    Response response = Response.newBuilder()
                            .addAllProcessedWords(processedWords)
                            .addAllVectorClock(toList(vectorClock))
                            .build();
                    response.writeTo(resultSocket.getOutputStream());
                    resultSocket.close();
                    resultServer.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void incrementClock(int processId) {
        vectorClock[processId]++;
    }

    private void updateClock(List<Integer> receivedClock) {
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock.get(i));
        }
    }

    private List<Integer> toList(int[] array) {
        List<Integer> list = new ArrayList<>();
        for (int value : array) {
            list.add(value);
        }
        return list;
    }
}
