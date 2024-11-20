package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.*;

public class Main {
    private static final int BASE_PORT = 10000;
    private static final int NUM_WORKERS = 5; //  Worker
    private final int[] vectorClock = new int[NUM_WORKERS + 1]; //

    public static void main(String[] args) {
        new Main().start();
    }

    public void start() {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Enter a paragraph:");
            String paragraph = scanner.nextLine();


            List<Word> words = new ArrayList<>();
            String[] wordArray = paragraph.split(" ");
            for (int i = 0; i < wordArray.length; i++) {
                words.add(Word.newBuilder()
                        .setText(wordArray[i])
                        .setOriginalIndex(i)
                        .build());
            }

            // assign to Workers
            Map<Integer, List<Word>> tasks = new HashMap<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                tasks.put(i, new ArrayList<>());
            }
            Random random = new Random();
            for (Word word : words) {
                int workerId = random.nextInt(NUM_WORKERS);
                tasks.get(workerId).add(word);
            }

            // send task to Worker
            for (int workerId = 0; workerId < NUM_WORKERS; workerId++) {
                sendMessageToWorker(workerId, tasks.get(workerId));
            }

            // waiting 15 seconds
            System.out.println("Main process is waiting for 15 seconds...");
            Thread.sleep(15000);

            // collect data after 15s
            System.out.println("Collecting results from workers...");
            List<Word> finalResults = new ArrayList<>();
            for (int workerId = 0; workerId < NUM_WORKERS; workerId++) {
                finalResults.addAll(collectFromWorker(workerId));
            }


            finalResults.sort(Comparator.comparingInt(Word::getOriginalIndex));

            // print final result
            StringBuilder result = new StringBuilder();
            for (Word word : finalResults) {
                result.append(word.getText()).append(" ");
            }
            System.out.println("Processed Paragraph: " + result.toString().trim());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMessageToWorker(int workerId, List<Word> words) {
        try (Socket socket = new Socket("localhost", BASE_PORT + workerId);
             OutputStream out = socket.getOutputStream()) {
            Message message = Message.newBuilder()
                    .addAllWords(words)
                    .addAllVectorClock(toList(vectorClock))
                    .build();
            message.writeTo(out);
        } catch (IOException e) {
            System.err.println("Failed to send task to Worker " + workerId);
            e.printStackTrace();
        }
        incrementClock(0); // update Vector Clock
    }

    private List<Word> collectFromWorker(int workerId) {
        try (Socket socket = new Socket("localhost", BASE_PORT + workerId + 1000);
             InputStream in = socket.getInputStream()) {

            Response response = Response.parseFrom(in);
            System.out.println("Success to collect result from Worker " + workerId);
            // update Vector Clock
            updateClock(response.getVectorClockList());


            return response.getProcessedWordsList();
        } catch (IOException e) {
            System.err.println("Failed to collect result from Worker " + workerId);
            e.printStackTrace();
        }
        return Collections.emptyList();
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


