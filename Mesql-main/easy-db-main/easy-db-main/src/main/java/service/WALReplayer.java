package service;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class WALReplayer {
    private static final String WAL_DIR = "wal";

    public void replay(LSMTree lsmTree) throws IOException {
        Files.createDirectories(Paths.get(WAL_DIR));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(WAL_DIR), "*.log")) {
            for (Path entry : stream) {
                replayLogFile(lsmTree, entry.toFile());
            }
        }
    }

    private void replayLogFile(LSMTree lsmTree, File logFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                String action = parts[0];
                String key = parts[1];
                String value = parts.length > 2 ? parts[2] : null;
                if ("SET".equals(action)) {
                    lsmTree.put(key, value);
                } else if ("RM".equals(action)) {
                    lsmTree.put(key, null); // 使用null表示删除
                }
            }
        }
    }
}
