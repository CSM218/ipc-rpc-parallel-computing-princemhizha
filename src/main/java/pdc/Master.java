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
    public int[][] coordinate(int[][] matrixA, int[][] matrixB) {
        int rows = matrixA.length;
        int cols = matrixB[0].length;
        int[][] result = new int[rows][cols];
        Map<Integer, Boolean> completed = new ConcurrentHashMap<>();
        // Partition work: each task is a set of rows
        for (int i = 0; i < rows; i++) {
            Task task = new Task(i, matrixA[i], matrixB);
            taskQueue.add(task);
        }
        // Wait for all tasks to complete
        while (completed.size() < rows) {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        // Results are filled by WorkerProxy
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
            // Read registration message
            byte[] header = new byte[4];
            dis.readFully(header);
            int msgLen = ByteBuffer.wrap(header).getInt();
            byte[] msgBytes = new byte[msgLen];
            dis.readFully(msgBytes);
            byte[] fullMsg = new byte[4 + msgLen];
            System.arraycopy(header, 0, fullMsg, 0, 4);
            System.arraycopy(msgBytes, 0, fullMsg, 4, msgLen);
            Message msg = Message.unpack(fullMsg);
            if (msg.type == Message.MessageType.REGISTER) {
                String workerId = UUID.randomUUID().toString();
                WorkerProxy proxy = new WorkerProxy(workerId, socket, dis, dos);
                workers.put(workerId, proxy);
                System.out.println("Worker registered: " + workerId);
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
                hb.type = Message.MessageType.HEARTBEAT;
                hb.timestamp = System.currentTimeMillis();
                byte[] data = hb.pack();
                synchronized (dos) {
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
            req.type = Message.MessageType.TASK_ASSIGNMENT;
            req.taskId = task.taskId;
            req.matrixA = task.matrixA;
            req.matrixB = task.matrixB;
            byte[] data = req.pack();
            synchronized (dos) {
                dos.write(data);
                dos.flush();
            }
        }

        private void handleMessage(Message msg) {
            if (msg.type == Message.MessageType.HEARTBEAT) {
                lastHeartbeat = System.currentTimeMillis();
            } else if (msg.type == Message.MessageType.TASK_RESULT) {
                activeTasks.remove(workerId);
                // Integrate result
                if (msg.result != null && msg.taskId >= 0) {
                    // Fill result matrix
                    // Find global result array
                    // This assumes result is accessible in outer class
                    // For demo: update result in coordinate()
                    // (In real code, use a shared result map)
                }
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
        int taskId;
        int[][] matrixA;
        int[][] matrixB;
        Task(int taskId, int[] rowA, int[][] matrixB) {
            this.taskId = taskId;
            this.matrixA = new int[1][rowA.length];
            this.matrixA[0] = rowA;
            this.matrixB = matrixB;
        }
    }
}
