package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final int BASE_PORT = 10000;
    private static final int NUM_WORKERS = 5; //
    private final int[] vectorClock = new int[NUM_WORKERS + 1];
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);

    public static void main(String[] args) {
        new Main().start();
    }

    public void start() {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Enter a paragraph:");
            String paragraph = scanner.nextLine();

            // Divide paragraphs into words
            List<Word> words = new ArrayList<>();
            String[] wordArray = paragraph.split(" ");
            for (int i = 0; i < wordArray.length; i++) {
                words.add(Word.newBuilder()
                        .setText(wordArray[i])
                        .setOriginalIndex(i)
                        .build());
            }

            // Randomly assign words to Workers
            Map<Integer, List<Word>> tasks = new HashMap<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                tasks.put(i, new ArrayList<>());
            }
            Random random = new Random();
            for (Word word : words) {
                int workerId = random.nextInt(NUM_WORKERS);
                tasks.get(workerId).add(word);
            }

            // A container for storing results
            ConcurrentMap<Integer, List<Word>> collectedResults = new ConcurrentHashMap<>();

            // 向每个 Worker 发送任务并监听其返回
            for (int workerId = 0; workerId < NUM_WORKERS; workerId++) {
                List<Word> taskWords = tasks.get(workerId);
                sendMessageToWorker(workerId, taskWords);

                // Start the listening thread and wait for the Worker to return the result
                int finalWorkerId = workerId;
                executor.submit(() -> listenForWorkerResponse(finalWorkerId, collectedResults));
            }

            // Close the thread pool and wait for all tasks to complete
            executor.shutdown();
            long start = System.currentTimeMillis();
            executor.awaitTermination(15, TimeUnit.SECONDS);
            long end = System.currentTimeMillis();
            long diff = (end - start)/1000;
            if (15-diff >0 ){
                TimeUnit.SECONDS.sleep(15-diff);
            }

            // Merge and sort the results
            List<Word> finalResults = new ArrayList<>();
            for (List<Word> workerResults : collectedResults.values()) {
                finalResults.addAll(workerResults);
            }
            finalResults.sort(Comparator.comparingInt(Word::getOriginalIndex));

            // Output final result
            StringBuilder result = new StringBuilder();
            for (Word word : finalResults) {
                result.append(word.getText()).append(" ");
            }
            System.out.println("Processed Paragraph: " + result.toString().trim());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMessageToWorker(int workerId, List<Word> words) throws IOException {
        try (Socket socket = new Socket("localhost", BASE_PORT + workerId);
             OutputStream out = socket.getOutputStream()) {
            Message message = Message.newBuilder()
                    .addAllWords(words)
                    .addAllVectorClock(toList(vectorClock))
                    .build();
            message.writeTo(out);
        }
        incrementClock(0); // The main process updates its own Vector Clock
    }

    private void listenForWorkerResponse(int workerId, ConcurrentMap<Integer, List<Word>> results) {
        try (ServerSocket serverSocket = new ServerSocket(BASE_PORT + workerId + 1000);
             Socket socket = serverSocket.accept();
             InputStream in = socket.getInputStream()) {

            Response response = Response.parseFrom(in);

            // update main process Vector Clock
            updateClock(response.getVectorClockList());

            // store Worker response data
            results.put(workerId, response.getProcessedWordsList());
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

