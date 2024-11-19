package com.distributed;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private static final int BASE_PORT = 10000;
    private static final int NUM_WORKERS = 3; // 修改为支持多个 Worker
    private final int[] vectorClock = new int[NUM_WORKERS + 1];
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);

    public static void main(String[] args) {
        new Main().start();
    }

    public void start() {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Enter a paragraph:");
            String paragraph = scanner.nextLine();

            // 切分段落为单词
            List<Word> words = new ArrayList<>();
            String[] wordArray = paragraph.split(" ");
            for (int i = 0; i < wordArray.length; i++) {
                words.add(Word.newBuilder()
                        .setText(wordArray[i])
                        .setOriginalIndex(i)
                        .build());
            }

            // 随机分配单词到 Workers
            Map<Integer, List<Word>> tasks = new HashMap<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                tasks.put(i, new ArrayList<>());
            }
            Random random = new Random();
            for (Word word : words) {
                int workerId = random.nextInt(NUM_WORKERS);
                tasks.get(workerId).add(word);
            }

            // 用于存储结果的容器
            ConcurrentMap<Integer, List<Word>> collectedResults = new ConcurrentHashMap<>();

            // 向每个 Worker 发送任务并监听其返回
            for (int workerId = 0; workerId < NUM_WORKERS; workerId++) {
                List<Word> taskWords = tasks.get(workerId);
                sendMessageToWorker(workerId, taskWords);

                // 启动监听线程等待 Worker 返回结果
                int finalWorkerId = workerId;
                executor.submit(() -> listenForWorkerResponse(finalWorkerId, collectedResults));
            }

            // 关闭线程池并等待所有任务完成
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            // 合并并排序结果
            List<Word> finalResults = new ArrayList<>();
            for (List<Word> workerResults : collectedResults.values()) {
                finalResults.addAll(workerResults);
            }
            finalResults.sort(Comparator.comparingInt(Word::getOriginalIndex));

            // 输出最终结果
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
        incrementClock(0); // 主进程更新自己的 Vector Clock
    }

    private void listenForWorkerResponse(int workerId, ConcurrentMap<Integer, List<Word>> results) {
        try (ServerSocket serverSocket = new ServerSocket(BASE_PORT + workerId + 1000);
             Socket socket = serverSocket.accept();
             InputStream in = socket.getInputStream()) {

            Response response = Response.parseFrom(in);

            // 更新主进程的 Vector Clock
            updateClock(response.getVectorClockList());

            // 存储 Worker 返回的结果
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

