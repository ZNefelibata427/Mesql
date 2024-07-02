package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.locks.*;
import java.util.zip.*;

public class NormalStore implements Store {

    // 文件名和模式定义
    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    // 内存表，用于存储数据的缓存
    private TreeMap<String, Command> memTable;

    // 索引，存储数据长度和偏移量
    private HashMap<String, CommandPos> index;

    // 数据目录
    private final String dataDir;

    // 读写锁，保证多线程并发安全
    private final ReadWriteLock indexLock;

    // 随机访问文件句柄
    private RandomAccessFile writerReader;

    // 文件大小阈值
    private static final int MAX_FILE_SIZE = 1024 * 1024 * 10; // 10MB
    private static final String WAL_FILE = "wal.log";
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<>();
        this.index = new HashMap<>();

        // 检查并创建数据目录
        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist, creating...");
            file.mkdirs();
            // 重新加载索引
            this.reloadIndex();
        }
        // 启动文件监控线程
        new Thread(this::monitorFileSize).start();

        // 创建WAL文件并重放
        createWalFile();
        replayWal();
    }

    // 写入WAL日志
    private void writeWal(String log) throws IOException {
        lock.writeLock().lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(WAL_FILE, true))) {
            writer.write(log);
            writer.newLine();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 重放WAL日志
    private void replayWal() {
        lock.writeLock().lock();
        try (BufferedReader reader = new BufferedReader(new FileReader(WAL_FILE))) {
            String log;
            while ((log = reader.readLine()) != null) {
                String[] parts = log.split(" ");
                String command = parts[0];
                String key = parts[1];
                String value = parts.length > 2 ? parts[2] : null;
                switch (command) {
                    case "SET":
                        memTable.put(key, new SetCommand(key, value));
                        break;
                    case "RM":
                        memTable.remove(key);
                        break;
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("WAL file not found, skipping replay");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 创建WAL文件
    private void createWalFile() {
        lock.writeLock().lock();
        try {
            File file = new File(WAL_FILE);
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 生成文件路径
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    // 文件大小监控线程
    private void monitorFileSize() {
        while (true) {
            try {
                File file = new File(this.genFilePath());
                if (file.length() > MAX_FILE_SIZE) {
                    rotateAndCompressFile(file);
                }
                Thread.sleep(5000); // 每5秒检查一次
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // 旋转并压缩文件!
    private void rotateAndCompressFile(File file) {
        lock.writeLock().lock();
        try {
            File rotatedFile = new File(file.getAbsolutePath() + System.currentTimeMillis());
            Files.move(file.toPath(), rotatedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            new Thread(() -> compressFile(rotatedFile)).start();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 压缩文件
    private void compressFile(File file) {
        try (FileInputStream fis = new FileInputStream(file);
             FileOutputStream fos = new FileOutputStream(file.getAbsolutePath() + ".gz");
             GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzos.write(buffer, 0, len);
            }
            file.delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 存储键值对
    @Override
    public void set(String key, String value) {
        try {
            // 创建一个包含键值对的SetCommand对象
            SetCommand command = new SetCommand(key, value);
            // 将命令对象序列化为字节数组
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // 获取写锁，确保在更新内存表和索引时没有其他线程进行读取或写入
            indexLock.writeLock().lock();

            // 将命令字节数组的长度写入文件（数据文件的路径由genFilePath方法生成）
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);

            // 将命令字节数组写入文件，并返回写入的位置（偏移量）
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

            // 将命令对象存储在内存表中
            memTable.put(key, command);

            // 创建一个CommandPos对象，记录命令在文件中的位置和长度
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);

            // 将命令的位置和长度存储在索引中，以便后续查找
            index.put(key, cmdPos);
        } catch (Throwable t) {
            // 如果发生任何异常，抛出RuntimeException
            throw new RuntimeException(t);
        } finally {
            // 无论是否发生异常，最终都会释放写锁
            indexLock.writeLock().unlock();
        }
        try {
            // 将操作记录写入WAL文件，以便在系统故障时进行恢复
            writeWal("SET " + key + " " + value);
        } catch (IOException e) {
            // 如果写入WAL文件时发生IO异常，打印堆栈跟踪
            e.printStackTrace();
        }
    }


    // 获取键值
    @Override
    public String get(String key) {
        try {
            // 获取读锁，确保在读取索引时没有其他线程进行写操作
            indexLock.readLock().lock();

            // 从索引中获取指定键的CommandPos对象
            CommandPos cmdPos = index.get(key);

            // 如果索引中不存在该键，返回null
            if (cmdPos == null) {
                return null;
            }

            // 从文件中读取命令字节数组
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

            // 将字节数组解析为JSON对象
            JSONObject value = JSONObject.parseObject(new String(commandBytes));

            // 将JSON对象转换为Command对象
            Command cmd = CommandUtil.jsonToCommand(value);

            // 如果命令是SetCommand，返回其值
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }

            // 如果命令是RmCommand，返回null
            if (cmd instanceof RmCommand) {
                return null;
            }
        } catch (Throwable t) {
            // 如果发生任何异常，抛出RuntimeException
            throw new RuntimeException(t);
        } finally {
            // 无论是否发生异常，最终都会释放读锁
            indexLock.readLock().unlock();
        }
        try {
            // 将删除操作记录写入WAL文件，以便在系统故障时进行恢复
            writeWal("RM " + key);
        } catch (IOException e) {
            // 如果写入WAL文件时发生IO异常，打印堆栈跟踪
            e.printStackTrace();
        }
        return null;
    }

    // 删除键值
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            indexLock.writeLock().lock();
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            memTable.remove(key);
            index.remove(key);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
        try {
            writeWal("RM " + key);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        // 关闭操作，这里可以加入一些资源清理代码
    }

    // 重新加载索引
    public void reloadIndex() {
        try (RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE)) {
            // 获取文件的长度
            long len = file.length();
            // 初始化起始位置为0
            long start = 0;
            // 将文件指针设置到起始位置
            file.seek(start);

            // 循环读取文件中的数据
            while (start < len) {
                // 读取命令长度
                int cmdLen = file.readInt();
                // 如果命令长度不合理（负数或超过文件剩余长度），跳出循环，防止内存溢出
                if (cmdLen < 0 || cmdLen > file.length() - start) {
                    break;
                }

                // 创建一个字节数组来存储命令数据
                byte[] bytes = new byte[cmdLen];
                // 读取命令数据
                file.read(bytes);
                // 将字节数组转换为JSON对象
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                // 将JSON对象转换为Command对象
                Command command = CommandUtil.jsonToCommand(value);
                // 增加4个字节（因为之前读取了一个int值，占4个字节）
                start += 4;

                // 如果命令对象不为空
                if (command != null) {
                    // 创建一个CommandPos对象来记录命令的位置信息
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    // 将命令键和值的位置存储到索引中
                    index.put(command.getKey(), cmdPos);

                    // 如果命令是SetCommand，将其存储到内存表中
                    if (command instanceof SetCommand) {
                        memTable.put(command.getKey(), command);
                    }
                    // 如果命令是RmCommand，将其从内存表中移除
                    else if (command instanceof RmCommand) {
                        memTable.remove(command.getKey());
                    }
                }
                // 增加命令长度，移动到下一个命令的起始位置
                start += cmdLen;
            }
            // 将文件指针设置到文件末尾
            file.seek(file.length());
        } catch (Exception e) {
            // 捕获任何异常，并打印堆栈跟踪
            e.printStackTrace();
        }
        // 打印重载索引后的索引内容
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }
}
