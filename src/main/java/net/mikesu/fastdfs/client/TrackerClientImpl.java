package net.mikesu.fastdfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import net.mikesu.fastdfs.FastdfsClientConfig;
import net.mikesu.fastdfs.command.CloseCmd;
import net.mikesu.fastdfs.command.Command;
import net.mikesu.fastdfs.command.GroupInfoCmd;
import net.mikesu.fastdfs.command.QueryDownloadCmd;
import net.mikesu.fastdfs.command.QueryUpdateCmd;
import net.mikesu.fastdfs.command.QueryUploadCmd;
import net.mikesu.fastdfs.command.StorageInfoCmd;
import net.mikesu.fastdfs.data.GroupInfo;
import net.mikesu.fastdfs.data.Result;
import net.mikesu.fastdfs.data.StorageInfo;
import net.mikesu.fastdfs.data.UploadStorage;

public class TrackerClientImpl implements TrackerClient {

    private Socket socket;

    private String host;

    private Integer port;

    private Integer connectTimeout = FastdfsClientConfig.DEFAULT_CONNECT_TIMEOUT * 1000;

    private Integer networkTimeout = FastdfsClientConfig.DEFAULT_NETWORK_TIMEOUT * 1000;

    public TrackerClientImpl(String address) {
        super();
        String[] hostport = address.split(":");
        this.host = hostport[0];
        this.port = Integer.valueOf(hostport[1]);
    }

    public TrackerClientImpl(String address, Integer connectTimeout, Integer networkTimeout) {
        this(address);
        this.connectTimeout = connectTimeout;
        this.networkTimeout = networkTimeout;
    }

    private void initSocket() throws IOException {
        if (socket == null || socket.isClosed()) {
            socket = new Socket();
            socket.setSoTimeout(networkTimeout);
            socket.connect(new InetSocketAddress(host, port), connectTimeout);
        }
    }

    @Override
    public void close() throws IOException {
        Command<Boolean> command = new CloseCmd();
        command.exec(socket);
        socket.getOutputStream().flush();
        socket.close();
        socket = null;
    }

    @Override
    public Result<UploadStorage> getUploadStorage() throws IOException {
        initSocket();
        Command<UploadStorage> command = new QueryUploadCmd();
        Result<UploadStorage> result = command.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<String> getUpdateStorageAddr(String group, String fileName) throws IOException {
        initSocket();
        Command<String> cmd = new QueryUpdateCmd(group, fileName);
        Result<String> result = cmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<String> getDownloadStorageAddr(String group, String fileName) throws IOException {
        initSocket();
        Command<String> cmd = new QueryDownloadCmd(group, fileName);
        Result<String> result = cmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<List<GroupInfo>> getGroupInfos() throws IOException {
        initSocket();
        Command<List<GroupInfo>> cmd = new GroupInfoCmd();
        Result<List<GroupInfo>> result = cmd.exec(socket);
        close();
        return result;
    }

    @Override
    public Result<List<StorageInfo>> getStorageInfos(String group) throws IOException {
        initSocket();
        Command<List<StorageInfo>> cmd = new StorageInfoCmd(group);
        Result<List<StorageInfo>> result = cmd.exec(socket);
        close();
        return result;
    }

}
