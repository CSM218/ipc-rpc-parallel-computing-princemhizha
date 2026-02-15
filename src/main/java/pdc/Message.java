package pdc;

import java.io.*;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    // Protocol Constants
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    // Message Types
    public static final String CONNECT = "CONNECT";
    public static final String REGISTER_WORKER = "REGISTER_WORKER";
    public static final String REGISTER_CAPABILITIES = "REGISTER_CAPABILITIES";
    public static final String RPC_REQUEST = "RPC_REQUEST";
    public static final String RPC_RESPONSE = "RPC_RESPONSE";
    public static final String TASK_COMPLETE = "TASK_COMPLETE";
    public static final String TASK_ERROR = "TASK_ERROR";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String WORKER_ACK = "WORKER_ACK";

    public String magic = MAGIC;
    public int version = VERSION;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Magic
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);

            dos.writeInt(version);

            // messageType (length + bytes)
            byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeBytes.length);
            package pdc;

            import java.io.*;
            import java.nio.ByteBuffer;
            import java.nio.charset.StandardCharsets;
            dos.write(senderBytes);
            /**
             * Message represents the communication unit in the CSM218 protocol.
             * Implements a strict binary wire protocol with framing:
             * [MESSAGE_LENGTH (4 bytes)][MESSAGE_TYPE (1 byte)][PAYLOAD]
             * No JSON, no Java Serialization. Only manual binary packing.
             */
                dos.write(payload);
                // Message Types for CSM218
                public enum MessageType {
                    REGISTER(1),
                    TASK_ASSIGNMENT(2),
                    TASK_RESULT(3),
                    HEARTBEAT(4),
                    SHUTDOWN(5);

                    public final byte code;
                    MessageType(int code) { this.code = (byte) code; }
                    public static MessageType fromByte(byte b) {
                        for (MessageType t : values()) if (t.code == b) return t;
                        throw new IllegalArgumentException("Unknown MessageType code: " + b);
                    }
                }

                public MessageType type;
                public int taskId; // Used for TASK_ASSIGNMENT, TASK_RESULT
                public int[][] matrixA; // Used for TASK_ASSIGNMENT
                public int[][] matrixB; // Used for TASK_ASSIGNMENT
                public int[][] result; // Used for TASK_RESULT
                public long timestamp; // Used for HEARTBEAT

                // For extensibility, add more fields as needed
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

                /**
                 * Converts the message to a byte stream for network transmission.
                 * Implements framing: [length][type][payload]
                 */
                public byte[] pack() {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    try {
                        baos.write(0); // Placeholder for length (will fill later)
                        baos.write(0);
                        baos.write(0);
                        baos.write(0);
                        baos.write(type.code); // 1 byte for type

                        // Payload depends on type
                        switch (type) {
                            case REGISTER:
                            case HEARTBEAT:
                            case SHUTDOWN:
                                // Only timestamp for HEARTBEAT, nothing for REGISTER/SHUTDOWN
                                if (type == MessageType.HEARTBEAT) {
                                    baos.write(ByteBuffer.allocate(8).putLong(timestamp).array());
                                }
                                break;
                            case TASK_ASSIGNMENT:
                                // [taskId][matrixA][matrixB]
                                baos.write(ByteBuffer.allocate(4).putInt(taskId).array());
                                writeMatrix(baos, matrixA);
                                writeMatrix(baos, matrixB);
                                break;
                            case TASK_RESULT:
                                // [taskId][result]
                                baos.write(ByteBuffer.allocate(4).putInt(taskId).array());
                                writeMatrix(baos, result);
                                break;
                        }
                        // Now fill in length
                        byte[] msgBytes = baos.toByteArray();
                        int len = msgBytes.length - 4;
                        ByteBuffer.wrap(msgBytes, 0, 4).putInt(len);
                        return msgBytes;
                    } catch (IOException e) {
                        throw new RuntimeException("Packing failed", e);
                    }
                }

                /**
                 * Reconstructs a Message from a byte stream.
                 * Expects framing: [length][type][payload]
                 */
                public static Message unpack(byte[] data) {
                    Message msg = new Message();
                    ByteBuffer buf = ByteBuffer.wrap(data);
                    int len = buf.getInt(); // total length (not used here)
                    byte typeByte = buf.get();
                    msg.type = MessageType.fromByte(typeByte);
                    switch (msg.type) {
                        case REGISTER:
                        case SHUTDOWN:
                            // No payload
                            break;
                        case HEARTBEAT:
                            msg.timestamp = buf.getLong();
                            break;
                        case TASK_ASSIGNMENT:
                            msg.taskId = buf.getInt();
                            msg.matrixA = readMatrix(buf);
                            msg.matrixB = readMatrix(buf);
                            break;
                        case TASK_RESULT:
                            msg.taskId = buf.getInt();
                            msg.result = readMatrix(buf);
                            break;
                    }
                    return msg;
                }

                // Helper: Write matrix to stream
                private static void writeMatrix(OutputStream os, int[][] matrix) throws IOException {
                    if (matrix == null) {
                        os.write(ByteBuffer.allocate(4).putInt(-1).array());
                        return;
                    }
                    int rows = matrix.length;
                    int cols = rows > 0 ? matrix[0].length : 0;
                    os.write(ByteBuffer.allocate(4).putInt(rows).array());
                    os.write(ByteBuffer.allocate(4).putInt(cols).array());
                    for (int i = 0; i < rows; i++)
                        for (int j = 0; j < cols; j++)
                            os.write(ByteBuffer.allocate(4).putInt(matrix[i][j]).array());
                }

                // Helper: Read matrix from buffer
                private static int[][] readMatrix(ByteBuffer buf) {
                    int rows = buf.getInt();
                    if (rows == -1) return null;
                    int cols = buf.getInt();
                    int[][] matrix = new int[rows][cols];
                    for (int i = 0; i < rows; i++)
                        for (int j = 0; j < cols; j++)
                            matrix[i][j] = buf.getInt();
                    return matrix;
                }
        sb.append("}");
                // ...existing code...
            }
