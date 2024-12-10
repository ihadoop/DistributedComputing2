package com.distributed;



public class Worker3 {

    private final int workerId;
    private int lamportClock = 0;  // Lamport clock for this worker
    private int times = 0;

    public Worker3(int workerId) {
        this.workerId = workerId;
    }

    public static void main(String[] args) {

        final int workerId = 2;  // Worker ID passed as a command-line argument
        Worker3 worker = new Worker3(workerId);
        worker.run();

    }

    public void run() {
        Pull pull = new Pull( 8887+workerId);
        Push push = new Push(8888+workerId);

        while (true) {

            byte[] message = pull.run();
            ByteArrayBuffer buffer = new ByteArrayBuffer(message);
            int receivedClock = buffer.getInt();
            lamportClock = Math.max(lamportClock, receivedClock) + 1;

            byte[] chunk = new byte[buffer.remaining()];
            chunk = buffer.getRemainingBytes();

            System.out.println("Receive Message  from WorkerId2");
            push.send(serializeChunk(chunk,lamportClock));
        }
    }

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