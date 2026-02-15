package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private String masterHost;
    private int masterPort;
    private final String studentId;
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private boolean running = true;

    public Worker() {
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN";
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        this();
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public static void main(String[] args) {
        String workerId = System.getenv("WORKER_ID") != null ? System.getenv("WORKER_ID")
                : "worker-" + System.currentTimeMillis();
        String masterHost = System.getenv("MASTER_HOST") != null ? System.getenv("MASTER_HOST") : "localhost";
        int masterPort = Integer.parseInt(System.getenv("MASTER_PORT") != null ? System.getenv("MASTER_PORT") : "9999");

        Worker worker = new Worker(workerId, masterHost, masterPort);
        worker.joinCluster();
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String host, int port) {
        this.masterHost = host;
        this.masterPort = port;
        joinCluster();
    }

    public void joinCluster() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

            // Send registration using new protocol
            Message reg = new Message();
            reg.type = Message.MessageType.REGISTER;
            byte[] regData = reg.pack();
            dos.write(regData);
            dos.flush();

            // Start heartbeat thread
            Thread heartbeatThread = new Thread(() -> {
                while (running) {
                    try {
                        Message heartbeat = new Message();
                        heartbeat.type = Message.MessageType.HEARTBEAT;
                        heartbeat.timestamp = System.currentTimeMillis();
                        byte[] hbData = heartbeat.pack();
                        synchronized (dos) {
                            dos.write(hbData);
                            dos.flush();
                        }
                        Thread.sleep(2000);
                    } catch (Exception e) {
                        // If socket is closed, exit thread
                        running = false;
                    }
                }
            });
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();

            // Communication loop
            while (running) {
                // Read message framing: [length][type][payload]
                byte[] header = new byte[4];
                dis.readFully(header);
                int msgLen = ByteBuffer.wrap(header).getInt();
                byte[] msgBytes = new byte[msgLen];
                dis.readFully(msgBytes);
                byte[] fullMsg = new byte[4 + msgLen];
                System.arraycopy(header, 0, fullMsg, 0, 4);
                System.arraycopy(msgBytes, 0, fullMsg, 4, msgLen);
                Message msg = Message.unpack(fullMsg);
                handleMessage(msg, dos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute() {
        // execute() in original test is non-blocking
        new Thread(this::joinCluster).start();
    }

    private void handleMessage(Message msg, DataOutputStream dos) throws IOException {
        if (msg.type == Message.MessageType.TASK_ASSIGNMENT) {
            taskExecutor.submit(() -> executeTask(msg, dos));
        } else if (msg.type == Message.MessageType.SHUTDOWN) {
            running = false;
        }
        // Heartbeat is handled by thread, no need to respond
    }

    private void executeTask(Message req, DataOutputStream dos) {
        try {
            // Compute assigned rows of matrixA * matrixB
            int[][] partialResult = null;
            if (req.matrixA != null && req.matrixB != null) {
                int rows = req.matrixA.length;
                int cols = req.matrixB[0].length;
                partialResult = new int[rows][cols];
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        int sum = 0;
                        for (int k = 0; k < req.matrixA[0].length; k++) {
                            sum += req.matrixA[i][k] * req.matrixB[k][j];
                        }
                        partialResult[i][j] = sum;
                    }
                }
            }
            // Send result
            Message completion = new Message();
            completion.type = Message.MessageType.TASK_RESULT;
            completion.taskId = req.taskId;
            completion.result = partialResult;
            byte[] data = completion.pack();
            synchronized (dos) {
                dos.write(data);
                dos.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
