package service;

import java.io.*;
import java.util.*;

public class LSMTree {
    private static final int MEMTABLE_SIZE_THRESHOLD = 1000;
    private TreeMap<String, String> memTable = new TreeMap<>();
    private List<File> sstables = new ArrayList<>();
    private String dataDir;

    public LSMTree(String dataDir) {
        this.dataDir = dataDir;
    }

    public synchronized void put(String key, String value) throws IOException {
        memTable.put(key, value);
        if (memTable.size() >= MEMTABLE_SIZE_THRESHOLD) {
            flushMemTable();
        }
    }

    public synchronized String get(String key) throws IOException {
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }
        for (File sstable : sstables) {
            String value = searchSSTable(sstable, key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private void flushMemTable() throws IOException {
        File sstable = new File(dataDir, "sstable_" + System.currentTimeMillis() + ".dat");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sstable))) {
            for (Map.Entry<String, String> entry : memTable.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        }
        sstables.add(sstable);
        memTable.clear();
    }

    private String searchSSTable(File sstable, String key) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(sstable))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                if (parts[0].equals(key)) {
                    return parts[1];
                }
            }
        }
        return null;
    }
}
