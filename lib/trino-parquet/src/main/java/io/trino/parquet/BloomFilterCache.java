package io.trino.parquet;

import org.apache.parquet.column.values.bloomfilter.BloomFilter;

import java.util.concurrent.ConcurrentHashMap;

public class BloomFilterCache {
    private static BloomFilterCache instance = new BloomFilterCache();
    public static BloomFilterCache getInstance() {
        return instance;
    }

    private ConcurrentHashMap<String, BloomFilter> bloomFilters = new ConcurrentHashMap<>();

    private BloomFilterCache() {

    }

    public BloomFilter get(String fileName, String blockId, String columnPath) {
        String key = this.getKey(fileName, blockId, columnPath);
        if(this.bloomFilters.containsKey(key)) {
            return this.bloomFilters.get(key);
        }
        return null;
    }

    public void put(String fileName, String blockId, String columnPath, BloomFilter bloomFilter) {
        String key = this.getKey(fileName, blockId, columnPath);
        this.bloomFilters.put(key,bloomFilter);
    }

    public boolean containsKey(String fileName, String blockId, String columnPath) {
        String key = this.getKey(fileName, blockId, columnPath);
        return this.bloomFilters.containsKey(key);
    }

    private String getKey(String fileName, String blockId, String columnPath) {
        return fileName + "_" + blockId + "_" + columnPath;
    }
}
