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
        try (Socket socket = new Socket(masterHost, masterPort);
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // Send registration
            Message reg = new Message();
            reg.messageType = Message.REGISTER_WORKER;
            reg.studentId = studentId;
            reg.timestamp = System.currentTimeMillis();
            byte[] regData = reg.pack();
            dos.writeInt(regData.length);
            dos.write(regData);
            dos.flush();

            // Handle communication loop
            while (running) {
                int len = dis.readInt();
                byte[] data = new byte[len];
                dis.readFully(data);
                Message msg = Message.unpack(data);
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
        if (Message.HEARTBEAT.equals(msg.messageType)) {
            // Respond to heartbeat
            Message response = new Message();
            response.messageType = Message.HEARTBEAT;
            response.studentId = studentId;
            response.timestamp = System.currentTimeMillis();
            byte[] data = response.pack();
            synchronized (dos) {
                dos.writeInt(data.length);
                dos.write(data);
                dos.flush();
            }
        } else if (Message.RPC_REQUEST.equals(msg.messageType)) {
            taskExecutor.submit(() -> executeTask(msg, dos));
        }
    }

    private void executeTask(Message req, DataOutputStream dos) {
        try {
            // Task format: rowA;fullB
            // Let's assume binary payload format: [lenA] [rowA...] [rowsB] [colsB]
            // [matrixB...]
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(req.payload));

            // Read Row A
            int lenA = dis.readInt();
            int[] rowA = new int[lenA];
            for (int i = 0; i < lenA; i++)
                rowA[i] = dis.readInt();

            // Read Full Matrix B
            int rowsB = dis.readInt();
            int colsB = dis.readInt();
            int[][] matrixB = new int[rowsB][colsB];
            for (int i = 0; i < rowsB; i++) {
                for (int j = 0; j < colsB; j++) {
                    matrixB[i][j] = dis.readInt();
                }
            }

            // Perform Multiplication: resultRow = rowA * matrixB
            // resultRow[j] = sum(rowA[k] * matrixB[k][j])
            int[] resultRow = new int[colsB];
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < lenA; k++) {
                    if (k < rowsB) {
                        sum += rowA[k] * matrixB[k][j];
                    }
                }
                resultRow[j] = sum;
            }

            // Serialize result
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(resultRow.length);
            for (int val : resultRow)
                out.writeInt(val);

            // Send completion
            Message completion = new Message();
            completion.messageType = Message.TASK_COMPLETE;
            completion.studentId = studentId;
            completion.timestamp = System.currentTimeMillis();
            completion.payload = baos.toByteArray();
            byte[] data = completion.pack();
            synchronized (dos) {
                dos.writeInt(data.length);
                dos.write(data);
                dos.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
