package io.trino.parquet;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.concurrent.ConcurrentHashMap;

public class ParquetMetadataCache {

    private static ParquetMetadataCache instance = new ParquetMetadataCache();

    private ConcurrentHashMap<String, ParquetMetadata> cache = new ConcurrentHashMap<>();

    private ParquetMetadataCache() {

    }

    public static ParquetMetadataCache getInstance() {
        return instance;
    }

    public boolean containsKey(String filePath) {
        return this.cache.containsKey(filePath);
    }

    public void put(String filePath, ParquetMetadata parquetMetadata) {
        this.cache.put(filePath, parquetMetadata);
    }

    public ParquetMetadata get(String filePath) {
        return this.cache.get(filePath);
    }
}
