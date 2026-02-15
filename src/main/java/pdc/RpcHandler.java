package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * RpcHandler provides a simple abstraction for sending and receiving Message objects
 * over a socket using the custom binary protocol.
 */
public class RpcHandler {
    private final DataInputStream dis;
    private final DataOutputStream dos;

    public RpcHandler(Socket socket) throws IOException {
        this.dis = new DataInputStream(socket.getInputStream());
        this.dos = new DataOutputStream(socket.getOutputStream());
    }

    public synchronized void send(Message msg) throws IOException {
        byte[] data = msg.pack();
        dos.writeInt(data.length);
        dos.write(data);
        dos.flush();
    }

    public synchronized Message receive() throws IOException {
        int len = dis.readInt();
        byte[] data = new byte[len];
        dis.readFully(data);
        return Message.unpack(data);
    }

    public void close() throws IOException {
        dis.close();
        dos.close();
    }
}
