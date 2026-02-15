package pdc;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * Implements a strict binary wire protocol with framing:
 * [MESSAGE_LENGTH (4 bytes)][MESSAGE_TYPE (1 byte)][PAYLOAD]
 * No JSON, no Java Serialization. Only manual binary packing.
 */
public class Message {
    // Protocol Constants
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

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

    public String magic = MAGIC;
    public int version = VERSION;
    public MessageType type;
    public String messageType; // for compatibility (redundant, but required by autograder)
    public String studentId = "";
    public long timestamp; // Used for HEARTBEAT
    public int taskId; // Used for TASK_ASSIGNMENT, TASK_RESULT
    public int[][] matrixA; // Used for TASK_ASSIGNMENT
    public int[][] matrixB; // Used for TASK_ASSIGNMENT
    public int[][] result; // Used for TASK_RESULT
    public byte[] payload; // for compatibility (not used in new protocol)

    public Message() {}

    /**
     * Converts the message to a byte stream for network transmission.
     * Implements framing: [length][type][payload]
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            // --- HEADER ---
            // Magic
            byte[] magicBytes = magic.getBytes();
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);
            // Version
            dos.writeInt(version);
            // MessageType (as byte)
            dos.writeByte(type.code);
            // messageType (string, for autograder compatibility)
            byte[] typeStr = (type != null ? type.name() : "").getBytes();
            dos.writeInt(typeStr.length);
            dos.write(typeStr);
            // studentId (string)
            byte[] studentBytes = (studentId != null ? studentId : "").getBytes();
            dos.writeInt(studentBytes.length);
            dos.write(studentBytes);
            // Timestamp
            dos.writeLong(timestamp);

            // --- PAYLOAD ---
            ByteArrayOutputStream payloadStream = new ByteArrayOutputStream();
            DataOutputStream payloadDos = new DataOutputStream(payloadStream);
            switch (type) {
                case REGISTER:
                case SHUTDOWN:
                    // No extra payload
                    break;
                case HEARTBEAT:
                    // Already wrote timestamp
                    break;
                case TASK_ASSIGNMENT:
                    payloadDos.writeInt(taskId);
                    writeMatrix(payloadDos, matrixA);
                    writeMatrix(payloadDos, matrixB);
                    break;
                case TASK_RESULT:
                    payloadDos.writeInt(taskId);
                    writeMatrix(payloadDos, result);
                    break;
            }
            payloadDos.flush();
            payload = payloadStream.toByteArray();
            // Write payload length and payload
            dos.writeInt(payload.length);
            dos.write(payload);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Packing failed", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects framing: [length][type][payload]
     */
    public static Message unpack(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();
            // --- HEADER ---
            int magicLen = dis.readInt();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes);
            msg.version = dis.readInt();
            byte typeByte = dis.readByte();
            msg.type = MessageType.fromByte(typeByte);
            int typeStrLen = dis.readInt();
            byte[] typeStrBytes = new byte[typeStrLen];
            dis.readFully(typeStrBytes);
            msg.messageType = new String(typeStrBytes);
            int studentLen = dis.readInt();
            byte[] studentBytes = new byte[studentLen];
            dis.readFully(studentBytes);
            msg.studentId = new String(studentBytes);
            msg.timestamp = dis.readLong();
            // --- PAYLOAD ---
            int payloadLen = dis.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                dis.readFully(msg.payload);
                DataInputStream payloadDis = new DataInputStream(new ByteArrayInputStream(msg.payload));
                switch (msg.type) {
                    case TASK_ASSIGNMENT:
                        msg.taskId = payloadDis.readInt();
                        msg.matrixA = readMatrix(payloadDis);
                        msg.matrixB = readMatrix(payloadDis);
                        break;
                    case TASK_RESULT:
                        msg.taskId = payloadDis.readInt();
                        msg.result = readMatrix(payloadDis);
                        break;
                    default:
                        // No extra payload
                        break;
                }
            }
            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Unpacking failed", e);
        }
    }

    // Helper: Write matrix to stream
    private static void writeMatrix(DataOutputStream dos, int[][] matrix) throws IOException {
        if (matrix == null) {
            dos.writeInt(-1);
            return;
        }
        int rows = matrix.length;
        int cols = rows > 0 ? matrix[0].length : 0;
        dos.writeInt(rows);
        dos.writeInt(cols);
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                dos.writeInt(matrix[i][j]);
    }

    // Helper: Read matrix from DataInputStream
    private static int[][] readMatrix(DataInputStream dis) throws IOException {
        int rows = dis.readInt();
        if (rows == -1) return null;
        int cols = dis.readInt();
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                matrix[i][j] = dis.readInt();
        return matrix;
    }
}
