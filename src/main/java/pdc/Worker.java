package pdc;

import java.nio.ByteBuffer;

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
            RpcHandler rpc = new RpcHandler(socket);
            // Send registration using new protocol
            Message reg = new Message();
            reg.type = Message.MessageType.REGISTER;
            rpc.send(reg);

            // Start heartbeat thread
            Thread heartbeatThread = new Thread(() -> {
                while (running) {
                    try {
                        Message heartbeat = new Message();
                        heartbeat.type = Message.MessageType.HEARTBEAT;
                        heartbeat.timestamp = System.currentTimeMillis();
                        rpc.send(heartbeat);
                        Thread.sleep(2000);
                    } catch (Exception e) {
                        running = false;
                    }
                }
            });
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();

            // Communication loop
            while (running) {
                Message msg = rpc.receive();
                handleMessage(msg, rpc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute() {
        // execute() in original test is non-blocking
        new Thread(this::joinCluster).start();
    }

    private void handleMessage(Message msg, RpcHandler rpc) {
        if (msg.type == Message.MessageType.TASK_ASSIGNMENT) {
            taskExecutor.submit(() -> executeTask(msg, rpc));
        } else if (msg.type == Message.MessageType.SHUTDOWN) {
            running = false;
        }
        // Heartbeat is handled by thread, no need to respond
    }

    private void executeTask(Message req, RpcHandler rpc) {
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
            rpc.send(completion);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
