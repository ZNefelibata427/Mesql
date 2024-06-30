package service;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class LogFileManager {
    private static final int MAX_LOG_SIZE = 1024 * 1024 * 10; // 10 MB
    private static final String LOG_DIR = "logs";
    private static final String LOG_FILE_PREFIX = "log_";
    private static final String COMPRESSED_FILE_EXTENSION = ".gz";
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private int currentLogIndex = 0;
    private File currentLogFile;
    private BufferedWriter writer;

    public LogFileManager() throws IOException {
        Files.createDirectories(Paths.get(LOG_DIR));
        rotateLogFile();
    }

    private void rotateLogFile() throws IOException {
        if (writer != null) {
            writer.close();
            compressLogFile(currentLogFile);
        }
        currentLogFile = new File(LOG_DIR, LOG_FILE_PREFIX + (++currentLogIndex) + ".log");
        writer = new BufferedWriter(new FileWriter(currentLogFile, true));
    }

    private void compressLogFile(File logFile) {
        executorService.submit(() -> {
            try {
                File compressedFile = new File(logFile.getPath() + COMPRESSED_FILE_EXTENSION);
                try (FileInputStream fis = new FileInputStream(logFile);
                     FileOutputStream fos = new FileOutputStream(compressedFile);
                     GZIPOutputStream gzos = new GZIPOutputStream(fos)) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = fis.read(buffer)) > 0) {
                        gzos.write(buffer, 0, len);
                    }
                }
                logFile.delete();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void writeLog(String logEntry) throws IOException {
        if (currentLogFile.length() > MAX_LOG_SIZE) {
            rotateLogFile();
        }
        writer.write(logEntry);
        writer.newLine();
        writer.flush();
    }

    public void shutdown() throws IOException {
        if (writer != null) {
            writer.close();
        }
        executorService.shutdown();
    }
}