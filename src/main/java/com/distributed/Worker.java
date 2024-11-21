package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;



public class Worker {
    private final int workerId;
    private final int[] vectorClock;
    private final Queue<PendingMessage> pendingMessages = new ConcurrentLinkedQueue<>();

    public Worker(int workerId, int numProcesses) {
        this.workerId = workerId;
        this.vectorClock = new int[numProcesses];
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(10000 + workerId)) {
            System.out.println("Worker " + workerId + " is running...");
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    handleClient(clientSocket);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (ObjectInputStream input = new ObjectInputStream(clientSocket.getInputStream());
             ObjectOutputStream output = new ObjectOutputStream(clientSocket.getOutputStream())) {

            // Receive words and vector clock
            @SuppressWarnings("unchecked")
            List<Word> receivedWords = (List<Word>) input.readObject();
            List<Integer> receivedClock = (List<Integer>) input.readObject();

            // Update vector clock
            updateVectorClock(receivedClock);

            // Process words
            List<Word> processedWords = processWords(receivedWords);
            System.out.println(processedWords);
            // Add to pending queue and check for causal delivery
            pendingMessages.add(new PendingMessage(processedWords, convertArrayToList(vectorClock)));
            processPendingQueue(output);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Word> processWords(List<Word> words) {
        List<Word> processedWords = new ArrayList<>();
        for (Word word : words) {
            String processedText = word.getText().toUpperCase();
            processedWords.add(word.toBuilder()
                    .setText(processedText)
                    .clearVectorClock()
                    .addAllVectorClock(convertArrayToList(vectorClock))
                    .setPosition(word.getPosition())
                    .build());
        }
        return processedWords;
    }


    private void processPendingQueue(ObjectOutputStream output) throws IOException {
        boolean delivered = true;
        while (delivered) { // Keep processing as long as there are deliverable messages
            delivered = false;
            Iterator<PendingMessage> iterator = pendingMessages.iterator();
            while (iterator.hasNext()) {
                PendingMessage message = iterator.next();
                if (canDeliver(message.vectorClock)) {
                    // Deliver the message
                    output.writeObject(message.words);
                    output.writeObject(message.vectorClock);
                    output.flush();
                    iterator.remove();
                    delivered = true;
                }
            }
        }
    }

    private boolean canDeliver(List<Integer> receivedClock) {
        for (int i = 0; i < vectorClock.length; i++) {
            if (receivedClock.get(i) > vectorClock[i]) {
                return false; // Dependency not met
            }
        }
        return true; // All dependencies are satisfied
    }


    private void updateVectorClock(List<Integer> receivedClock) {
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock.get(i));
        }
        incrementClock(workerId);
    }

    private void incrementClock(int processId) {
        vectorClock[processId]++;
    }

    private List<Integer> convertArrayToList(int[] array) {
        List<Integer> list = new ArrayList<>();
        for (int value : array) {
            list.add(value);
        }
        return list;
    }

    private static class PendingMessage {
        List<Word> words;
        List<Integer> vectorClock;

        public PendingMessage(List<Word> words, List<Integer> vectorClock) {
            this.words = words;
            this.vectorClock = vectorClock;
        }
    }

    public static void main(String[] args) {


       int workerId = 4;
        int numProcesses = 5;

        Worker worker = new Worker(workerId, numProcesses);
        worker.start();
/**
        for (int i = 0; i < numProcesses; i++) {
            final int workerId = i;
            new Thread(()->{
                Worker worker = new Worker(workerId, numProcesses);
                worker.start();
            }).start();
        }
*/
    }
}
