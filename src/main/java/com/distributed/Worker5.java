package com.distributed;



public class Worker5 {

    private final int workerId;
    private int lamportClock = 0;  // Lamport clock for this worker
    private int times = 0;

    public Worker5(int workerId) {
        this.workerId = workerId;
    }

    public static void main(String[] args) {

        final int workerId = 4;  // Worker ID passed as a command-line argument
        Worker5 worker = new Worker5(workerId);
        worker.run();

    }

    public void run() {
        Pull pull = new Pull( 8887+workerId);
        Push push = new Push(9999);

        while (true) {

            byte[] message = pull.run();
            ByteArrayBuffer buffer = new ByteArrayBuffer(message);
            int receivedClock = buffer.getInt();
            lamportClock = Math.max(lamportClock, receivedClock) + 1;

            byte[] chunk = new byte[buffer.remaining()];
            chunk = buffer.getRemainingBytes(); // Get the chunk of data

            System.out.println("Receive Message  from WorkerId4");
            push.send(serializeChunk(chunk,lamportClock));
        }
    }

    // Serialize the chunk and attach the Lamport clock value
    private byte[] serializeChunk(byte[] chunk, int lamportClock) {
        ByteArrayBuffer buffer = new ByteArrayBuffer(chunk.length + Integer.BYTES);
        buffer.putInt(lamportClock);  // Add Lamport clock (4 bytes)
        buffer.put(chunk);  // Add the chunk
        return buffer.getArray();
    }
    // Process the chunk of data (for now, we convert it to uppercase)
    private String processChunk(byte[] chunk) {
        // For example: just convert the bytes to uppercase
        String processed = new String(chunk).toUpperCase();
        //System.out.println("---" + processed);
        return processed;
    }
}