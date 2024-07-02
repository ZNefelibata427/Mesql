package service;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class LogFileManager {
    // 最大日志文件大小为10MB
    private static final int MAX_LOG_SIZE = 1024 * 1024 * 10; // 10 MB

    // 日志文件目录
    private static final String LOG_DIR = "logs";

    // 日志文件前缀
    private static final String LOG_FILE_PREFIX = "log_";

    // 压缩文件扩展名
    private static final String COMPRESSED_FILE_EXTENSION = ".gz";

    // 用于压缩日志文件的单线程执行器
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    // 当前日志文件索引
    private int currentLogIndex = 0;

    // 当前日志文件
    private File currentLogFile;

    // 日志文件写入器
    private BufferedWriter writer;

    // 构造函数，创建日志目录并初始化日志文件
    public LogFileManager() throws IOException {
        Files.createDirectories(Paths.get(LOG_DIR)); // 创建日志目录
        rotateLogFile(); // 初始化第一个日志文件
    }

    // 轮换日志文件
    private void rotateLogFile() throws IOException {
        if (writer != null) {
            writer.close(); // 关闭当前日志文件写入器
            compressLogFile(currentLogFile); // 压缩旧日志文件
        }
        // 创建新的日志文件
        currentLogFile = new File(LOG_DIR, LOG_FILE_PREFIX + (++currentLogIndex) + ".log");
        writer = new BufferedWriter(new FileWriter(currentLogFile, true)); // 初始化新的写入器
    }

    // 异步压缩日志文件
    private void compressLogFile(File logFile) {
        executorService.submit(() -> {
            try {
                // 创建压缩文件
                File compressedFile = new File(logFile.getPath() + COMPRESSED_FILE_EXTENSION);
                try (FileInputStream fis = new FileInputStream(logFile);
                     FileOutputStream fos = new FileOutputStream(compressedFile);
                     GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
                    byte[] buffer = new byte[1024];
                    int len;
                    // 读取日志文件并写入压缩文件
                    while ((len = fis.read(buffer)) > 0) {
                        gzos.write(buffer, 0, len);
                    }
                }
                // 删除旧日志文件
                logFile.delete();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    // 写入日志条目
    public void writeLog(String logEntry) throws IOException {
        if (currentLogFile.length() > MAX_LOG_SIZE) {
            rotateLogFile(); // 如果日志文件大小超过阈值，则轮换日志文件
        }
        writer.write(logEntry); // 写入日志条目
        writer.newLine();
        writer.flush();
    }

    // 关闭日志文件管理器
    public void shutdown() throws IOException {
        if (writer != null) {
            writer.close(); // 关闭当前日志文件写入器
        }
        executorService.shutdown(); // 关闭执行器
    }
}
