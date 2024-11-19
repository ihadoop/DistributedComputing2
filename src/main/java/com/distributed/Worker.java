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

                    // 接收主进程的任务
                    Message message = Message.parseFrom(in);

                    // 更新 Vector Clock
                    updateClock(message.getVectorClockList());

                    // 处理任务
                    List<Word> processedWords = processWords(message.getWordsList());

                    // 增加 Vector Clock
                    incrementClock(workerId);

                    // 返回处理结果
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
                    .setText(word.getText().toUpperCase()) // 转为大写
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
        int numProcess = 3;
        int workerId = 2;
        Worker worker = new Worker(workerId, numProcess); // 默认支持 3 Worker
        try {
            worker.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}