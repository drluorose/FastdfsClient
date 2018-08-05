package net.mikesu.fastdfs.client;

import net.mikesu.fastdfs.FastdfsClientConfig;
import net.mikesu.fastdfs.command.ByteUploadCmd;
import net.mikesu.fastdfs.command.CloseCmd;
import net.mikesu.fastdfs.command.Command;
import net.mikesu.fastdfs.command.DeleteCmd;
import net.mikesu.fastdfs.command.GetMetaDataCmd;
import net.mikesu.fastdfs.command.SetMetaDataCmd;
import net.mikesu.fastdfs.command.UploadCmd;
import net.mikesu.fastdfs.data.Result;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

public class StorageClientImpl implements StorageClient {

    private Socket socket;

    private String host;

    private Integer port;

    private Integer connectTimeout = FastdfsClientConfig.DEFAULT_CONNECT_TIMEOUT * 1000;

    private Integer networkTimeout = FastdfsClientConfig.DEFAULT_NETWORK_TIMEOUT * 1000;

    private void initSocket() throws IOException {
        if (socket == null || socket.isClosed()) {
            socket = new Socket();
            socket.setSoTimeout(networkTimeout);
            socket.connect(new InetSocketAddress(host, port), connectTimeout);
        }
    }

    public StorageClientImpl(String address) {
        super();
        String[] hostport = address.split(":");
        this.host = hostport[0];
        this.port = Integer.valueOf(hostport[1]);
    }

    public StorageClientImpl(String address, Integer connectTimeout, Integer networkTimeout) {
        this(address);
        this.connectTimeout = connectTimeout;
        this.networkTimeout = networkTimeout;
    }

    @Override
    public void close() throws IOException {
        Command<Boolean> command = new CloseCmd();
        command.exec(socket);
        socket.close();
        socket = null;
    }

    @Override
    public Result<String> upload(File file, String fileName, byte storePathIndex) throws IOException {
        initSocket();
        UploadCmd uploadCmd = new UploadCmd(file, fileName, storePathIndex);
        Result<String> result = uploadCmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<String> upload(byte[] file, String fileName, byte storePathIndex) throws IOException {
        initSocket();
        ByteUploadCmd uploadCmd = new ByteUploadCmd(file, fileName, storePathIndex);
        Result<String> result = uploadCmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<Boolean> delete(String group, String fileName) throws IOException {
        initSocket();
        DeleteCmd deleteCmd = new DeleteCmd(group, fileName);
        Result<Boolean> result = deleteCmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<Boolean> setMeta(String group, String fileName,
                                   Map<String, String> meta) throws IOException {
        initSocket();
        SetMetaDataCmd setMetaDataCmd = new SetMetaDataCmd(group, fileName, meta);
        Result<Boolean> result = setMetaDataCmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<Map<String, String>> getMeta(String group, String fileName)
        throws IOException {
        initSocket();
        GetMetaDataCmd getMetaDataCmd = new GetMetaDataCmd(group, fileName);
        Result<Map<String, String>> result = getMetaDataCmd.exec(socket);
        this.close();
        return result;
    }

}
