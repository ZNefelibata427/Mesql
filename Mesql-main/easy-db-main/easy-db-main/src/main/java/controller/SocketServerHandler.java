package controller;

import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import service.NormalStore;
import service.Store;
import utils.LoggerUtil;

import java.io.*;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// SocketServerHandler类实现Runnable接口，可以作为一个线程运行
public class SocketServerHandler implements Runnable {
    // 日志记录器，用于记录日志信息
    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerHandler.class);

    // 套接字，用于与客户端通信
    private Socket socket;

    // 存储服务，用于处理数据存储相关操作
    private Store store;

    // 构造方法，初始化socket和store
    public SocketServerHandler(Socket socket, Store store) {
        this.socket = socket;
        this.store = store;
    }

    // 实现Runnable接口的run方法，线程执行时会调用此方法
    @Override
    public void run() {
        try (
                // 创建对象输入流和输出流，用于读取和发送对象
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())
        ) {
            // 读取客户端发送的请求对象
            ActionDTO dto = (ActionDTO) ois.readObject();
            LoggerUtil.debug(LOGGER, "[SocketServerHandler][ActionDTO]: {}", dto.toString());

            // 根据请求类型执行相应操作
            if (dto.getType() == ActionTypeEnum.GET) {
                // 处理GET请求
                String value = this.store.get(dto.getKey());
                RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
                oos.writeObject(resp);
                oos.flush();
            } else if (dto.getType() == ActionTypeEnum.SET) {
                // 处理SET请求
                this.store.set(dto.getKey(), dto.getValue());
                RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
                oos.writeObject(resp);
                oos.flush();
            } else if (dto.getType() == ActionTypeEnum.RM) {
                // 处理RM请求
                this.store.rm(dto.getKey());
                RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
                oos.writeObject(resp);
                oos.flush();
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // 最后关闭套接字
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}