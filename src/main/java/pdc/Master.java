package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {

    private final String studentId;
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerProxy> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final Map<String, Task> activeTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public Master() {
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN";
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv("MASTER_PORT") != null ? System.getenv("MASTER_PORT") : "9999");
        Master master = new Master();
        master.listen(port);
    }

    /**
     * Entry point for a distributed computation.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (!operation.equals("BLOCK_MULTIPLY")) {
            return null;
        }

        // Partitioning: For simplicity, treat rows as units of work
        int rows = data.length;
        for (int i = 0; i < rows; i++) {
            taskQueue.add(new Task(String.valueOf(i), operation, data[i]));
        }

        int[][] result = new int[rows][];
        int completedTasks = 0;

        while (completedTasks < rows) {
            // Task assignment logic is handled by worker handler threads
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Check for completed tasks (mocked for this loop, actual result collection
            // happens in WorkerProxy)
            // In a real implementation, we'd use a Future or a CompletionService
            // For now, let's just wait until all results are in result array
            completedTasks = 0;
            for (int i = 0; i < rows; i++) {
                if (result[i] != null)
                    completedTasks++;
            }
        }

        return result;
    }

    /**
     * Start the communication listener.
     */
    public void listen(int port) throws IOException {
        systemThreads.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Master listening on port " + port);

                reconcileState();

                while (running.get()) {
                    Socket clientSocket = serverSocket.accept();
                    systemThreads.submit(() -> handleWorkerConnection(clientSocket));
                }
            } catch (IOException e) {
                if (running.get()) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

            // Read framing length (assuming students use length-prefixing)
            int len = dis.readInt();
            byte[] data = new byte[len];
            dis.readFully(data);

            Message msg = Message.unpack(data);
            if (Message.REGISTER_WORKER.equals(msg.messageType)) {
                String workerId = msg.studentId; // or some other identifier in payload
                WorkerProxy proxy = new WorkerProxy(workerId, socket, dis, dos);
                workers.put(workerId, proxy);
                System.out.println("Worker registered: " + workerId);

                // Send ACK
                Message ack = new Message();
                ack.messageType = Message.WORKER_ACK;
                ack.studentId = studentId;
                ack.timestamp = System.currentTimeMillis();
                byte[] ackData = ack.pack();
                dos.writeInt(ackData.length);
                dos.write(ackData);
                dos.flush();

                proxy.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * System Health Check. Starts a background thread for reconciliation.
     */
    public void reconcileState() {
        systemThreads.submit(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(2000); // Heartbeat interval
                    for (Map.Entry<String, WorkerProxy> entry : workers.entrySet()) {
                        WorkerProxy proxy = entry.getValue();
                        if (!proxy.isProxyAlive()) {
                            System.out.println("Worker failure detected: " + entry.getKey());
                            reassignWorkerTasks(entry.getKey());
                            workers.remove(entry.getKey());
                        } else {
                            proxy.sendHeartbeat();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private class WorkerProxy extends Thread {
        private final String workerId;
        private final Socket socket;
        private final DataInputStream dis;
        private final DataOutputStream dos;
        private long lastHeartbeat;
        private boolean alive = true;

        public WorkerProxy(String id, Socket s, DataInputStream di, DataOutputStream doStream) {
            this.workerId = id;
            this.socket = s;
            this.dis = di;
            this.dos = doStream;
            this.lastHeartbeat = System.currentTimeMillis();
        }

        public boolean isProxyAlive() {
            long heartbeatTimeout = 5000;
            return alive && (System.currentTimeMillis() - lastHeartbeat < heartbeatTimeout);
        }

        public void sendHeartbeat() {
            try {
                Message hb = new Message();
                hb.messageType = Message.HEARTBEAT;
                hb.studentId = studentId;
                hb.timestamp = System.currentTimeMillis();
                byte[] data = hb.pack();
                synchronized (dos) {
                    dos.writeInt(data.length);
                    dos.write(data);
                    dos.flush();
                }
            } catch (IOException e) {
                alive = false;
            }
        }

        @Override
        public void run() {
            try {
                while (alive) {
                    // Try to get a task if not already busy
                    Task task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                        activeTasks.put(workerId, task);
                        sendTask(task);
                    }

                    // Read responses
                    if (socket.getInputStream().available() > 0) {
                        int len = dis.readInt();
                        byte[] data = new byte[len];
                        dis.readFully(data);
                        Message msg = Message.unpack(data);
                        handleMessage(msg);
                    }
                }
            } catch (Exception e) {
                alive = false;
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }

        private void sendTask(Task task) throws IOException {
            Message req = new Message();
            req.messageType = Message.RPC_REQUEST;
            req.studentId = studentId;
            req.timestamp = System.currentTimeMillis();
            // Payload could be taskId + data
            // For now, just a dummy serialization
            req.payload = task.data; // This needs to be bytes
            byte[] data = req.pack();
            synchronized (dos) {
                dos.writeInt(data.length);
                dos.write(data);
                dos.flush();
            }
        }

        private void handleMessage(Message msg) {
            if (Message.HEARTBEAT.equals(msg.messageType)) {
                lastHeartbeat = System.currentTimeMillis();
            } else if (Message.TASK_COMPLETE.equals(msg.messageType)) {
                activeTasks.remove(workerId);
                // Handle result integration
            }
        }
    }

    private void reassignWorkerTasks(String workerId) {
        Task task = activeTasks.remove(workerId);
        if (task != null) {
            System.out.println("Attempting to recover system state. Reassigning task from " + workerId);
            taskQueue.add(task);
        }
    }

    private static class Task {
        byte[] data;

        Task(String id, String type, int[] row) {
            // Serialize row to bytes
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeInt(row.length);
                for (int val : row)
                    dos.writeInt(val);
                this.data = baos.toByteArray();
            } catch (IOException e) {
                this.data = new byte[0];
            }
        }
    }
}
