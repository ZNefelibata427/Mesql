package service;

import java.io.*;
import java.util.*;

public class LSMTree {
    // 内存表大小阈值，当内存表中键值对数量达到此阈值时会将其刷入磁盘
    private static final int MEMTABLE_SIZE_THRESHOLD = 1000;

    // 内存表，使用TreeMap来存储键值对
    private TreeMap<String, String> memTable = new TreeMap<>();

    // SSTable文件列表，用于存储刷入磁盘的SSTable文件
    private List<File> sstables = new ArrayList<>();

    // 数据目录，存储SSTable文件的目录
    private String dataDir;

    // 构造方法，初始化数据目录
    public LSMTree(String dataDir) {
        this.dataDir = dataDir;
    }

    // 向LSM树中添加键值对
    public synchronized void put(String key, String value) throws IOException {
        // 将键值对插入内存表
        memTable.put(key, value);

        // 如果内存表大小超过阈值，将其刷入磁盘
        if (memTable.size() >= MEMTABLE_SIZE_THRESHOLD) {
            flushMemTable();
        }
    }

    // 从LSM树中获取键值对
    public synchronized String get(String key) throws IOException {
        // 先从内存表中查找键值对
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        // 如果内存表中没有，从SSTable文件中查找键值对
        for (File sstable : sstables) {
            String value = searchSSTable(sstable, key);
            if (value != null) {
                return value;
            }
        }

        // 如果内存表和SSTable文件中都没有，返回null
        return null;
    }

    // 将内存表刷入磁盘
    private void flushMemTable() throws IOException {
        // 创建新的SSTable文件
        File sstable = new File(dataDir, "sstable_" + System.currentTimeMillis() + ".dat");

        // 将内存表中的键值对写入SSTable文件
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sstable))) {
            for (Map.Entry<String, String> entry : memTable.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        }

        // 将新的SSTable文件添加到SSTable文件列表中
        sstables.add(sstable);

        // 清空内存表
        memTable.clear();
    }

    // 在SSTable文件中查找键值对
    private String searchSSTable(File sstable, String key) throws IOException {
        // 读取SSTable文件
        try (BufferedReader reader = new BufferedReader(new FileReader(sstable))) {
            String line;

            // 遍历SSTable文件中的每一行
            while ((line = reader.readLine()) != null) {
                // 将行分割成键和值
                String[] parts = line.split("=");

                // 如果找到了匹配的键，返回对应的值
                if (parts[0].equals(key)) {
                    return parts[1];
                }
            }
        }

        // 如果在SSTable文件中没有找到匹配的键，返回null
        return null;
    }
}
