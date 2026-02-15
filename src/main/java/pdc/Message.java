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
            dos.write(typeBytes);

            // studentId (length + bytes)
            byte[] senderBytes = studentId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);

            dos.writeLong(timestamp);

            // payload (length + bytes, allow null)
            if (payload == null) {
                dos.writeInt(-1);
            } else {
                dos.writeInt(payload.length);
                dos.write(payload);
            }

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Packing failed", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            Message msg = new Message();

            // magic
            int magicLen = dis.readInt();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            msg.version = dis.readInt();

            // messageType
            int typeLen = dis.readInt();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);

            // studentId
            int senderLen = dis.readInt();
            byte[] senderBytes = new byte[senderLen];
            dis.readFully(senderBytes);
            msg.studentId = new String(senderBytes, StandardCharsets.UTF_8);

            msg.timestamp = dis.readLong();

            // payload
            int payloadLen = dis.readInt();
            if (payloadLen >= 0) {
                byte[] payloadBytes = new byte[payloadLen];
                dis.readFully(payloadBytes);
                msg.payload = payloadBytes;
            } else {
                msg.payload = null;
            }

            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Unpacking failed", e);
        }
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"magic\":\"").append(magic).append("\",");
        sb.append("\"version\":").append(version).append(",");
        sb.append("\"messageType\":\"").append(messageType).append("\",");
        sb.append("\"studentId\":\"").append(studentId).append("\",");
        sb.append("\"timestamp\":").append(timestamp).append(",");
        sb.append("\"payload\":\"").append(payload != null ? Base64.getEncoder().encodeToString(payload) : "")
                .append("\"");
        sb.append("}");
        return sb.toString();
    }

    public static Message parse(String json) {
        Message msg = new Message();
        if (json.contains("\"magic\":\""))
            msg.magic = extract(json, "\"magic\":\"", "\"");
        if (json.contains("\"version\":")) {
            String v = extract(json, "\"version\":", ",");
            msg.version = Integer.parseInt(v.trim());
        }
        if (json.contains("\"messageType\":\""))
            msg.messageType = extract(json, "\"messageType\":\"", "\"");
        if (json.contains("\"studentId\":\""))
            msg.studentId = extract(json, "\"studentId\":\"", "\"");
        if (json.contains("\"timestamp\":")) {
            String ts = extract(json, "\"timestamp\":", ",");
            if (ts.endsWith("}"))
                ts = ts.substring(0, ts.length() - 1);
            msg.timestamp = Long.parseLong(ts.trim());
        }
        if (json.contains("\"payload\":\"")) {
            String p = extract(json, "\"payload\":\"", "\"");
            if (!p.isEmpty())
                msg.payload = Base64.getDecoder().decode(p);
        }
        return msg;
    }

    private static String extract(String json, String start, String end) {
        int s = json.indexOf(start) + start.length();
        int e = json.indexOf(end, s);
        if (e == -1) {
            e = json.indexOf("}", s);
            if (e == -1)
                e = json.length();
        }
        return json.substring(s, e);
    }
}
