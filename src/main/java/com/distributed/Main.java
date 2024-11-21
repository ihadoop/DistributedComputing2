package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.*;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final int NUM_WORKERS = 5; // Total number of workers
    private final int[] vectorClock = new int[NUM_WORKERS]; // Main's vector clock

    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }

    public void run() {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter a paragraph:");
            String paragraph = scanner.nextLine();

            // Split paragraph into words
            String[] words = paragraph.split("\\s+");
            List<List<Word>> wordBatches = distributeWords(words);

            // Send words to workers
            ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
            List<Socket> workerSockets = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                int workerId = i;
                executor.submit(() -> {
                    try {
                        Socket socket = new Socket("localhost", 10000 + workerId);
                        synchronized (workerSockets) {
                            workerSockets.add(socket); // Keep socket open for later communication
                        }
                        ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                        incrementClock(0); // Increment Main's clock
                        output.writeObject(wordBatches.get(workerId)); // Send batch to the worker
                        output.writeObject(convertArrayToList(vectorClock));
                        output.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            // Wait 15 seconds before collecting results
            System.out.println("Waiting 15 seconds before collecting results...");
            Thread.sleep(15000);

            // Collect results from workers
            List<Word> collectedResults = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                try (Socket socket = workerSockets.get(i);
                     ObjectInputStream input = new ObjectInputStream(socket.getInputStream())) {

                    @SuppressWarnings("unchecked")
                    List<Word> processedWords = (List<Word>) input.readObject();
                    collectedResults.addAll(processedWords);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Sort results by position (original order), and use vector clock as secondary criterion
            collectedResults.sort((word1, word2) -> {
                // Sort primarily by original position
                int posComparison = Integer.compare(word1.getPosition(), word2.getPosition());
                if (posComparison != 0) {
                    return posComparison;
                }

                // If positions are the same, sort by vector clock for causality
                List<Integer> clock1 = word1.getVectorClockList();
                List<Integer> clock2 = word2.getVectorClockList();
                for (int i = 0; i < Math.min(clock1.size(), clock2.size()); i++) {
                    int cmp = Integer.compare(clock1.get(i), clock2.get(i));
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return 0;
            });

            // Print the final results
            System.out.println("Collected results:");
            collectedResults.forEach(word -> System.out.print(word.getText() + " "));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<List<Word>> distributeWords(String[] words) {
        List<List<Word>> batches = new ArrayList<>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            batches.add(new ArrayList<>());
        }

        Random random = new Random();
        for (int i = 0; i < words.length; i++) {
            int workerId = random.nextInt(NUM_WORKERS); // Randomly assign a word to a worker
            batches.get(workerId).add(Word.newBuilder()
                    .setText(words[i])
                    .addAllVectorClock(convertArrayToList(vectorClock))
                    .setPosition(i) // Preserve the original position
                    .build());
        }
        return batches;
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
}
