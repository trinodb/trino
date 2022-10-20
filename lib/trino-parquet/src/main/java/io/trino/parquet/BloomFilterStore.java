package io.trino.parquet;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import org.apache.parquet.Log;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BloomFilterStore {
    private static final Logger LOG = Logger.get(BloomFilterStore.class);

    private BloomFilterCache cache;
    //private final Map<ColumnPath, BloomFilter> cache = new HashMap<>();
    private final ParquetDataSource dataSource;

    @VisibleForTesting
    BloomFilterStore()
    {
        this.dataSource = null;
    }

    public BloomFilterStore(ParquetDataSource dataSource)
    {
        this.dataSource = dataSource;
        this.cache = BloomFilterCache.getInstance();
    }

    public Optional<BloomFilter> readBloomFilter(String filename, String blockId, ColumnChunkMetaData columnMetaData)
    {
        if (!bloomFilterEnabled(columnMetaData)) {
            return Optional.empty();
        }

        if (cache.containsKey(filename, blockId, columnMetaData.getPath().toDotString())) {
            LOG.info("Bloomfilter found in cache " + filename + "_" + blockId + "_" + columnMetaData.getPath().toDotString());
            return Optional.ofNullable(cache.get(filename, blockId, columnMetaData.getPath().toDotString()));
        }

        try {
            Optional<BloomFilter> bloomFilterOptional = readBloomFilterInternal(columnMetaData);
            if (bloomFilterOptional.isEmpty()) {
                cache.put(filename, blockId, columnMetaData.getPath().toDotString(), null);
            }
            else {
                cache.put(filename, blockId, columnMetaData.getPath().toDotString(), bloomFilterOptional.get());
            }
            return bloomFilterOptional;
        }
        catch (IOException exception) {
            LOG.error(exception, "Failed to read Bloom filter data");
        }

        return Optional.empty();
    }

    public static boolean bloomFilterEnabled(ColumnChunkMetaData columnMetaData)
    {
        return columnMetaData.getBloomFilterOffset() > 0;
    }

    private Optional<BloomFilter>   readBloomFilterInternal(ColumnChunkMetaData columnMetaData)
            throws IOException
    {
        Instant start = Instant.now();
        if (!bloomFilterEnabled(columnMetaData)) {
            return Optional.empty();
        }

        long offset = columnMetaData.getBloomFilterOffset();
        SeekableInputStream seekableInputStream = dataSource.seekableInputStream();
        seekableInputStream.seek(offset);

        BloomFilterHeader bloomFilterHeader = Util.readBloomFilterHeader(seekableInputStream);

        int numBytes = bloomFilterHeader.getNumBytes();
        if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
            return Optional.empty();
        }
        if (!bloomFilterHeader.getHash().isSetXXHASH() || !bloomFilterHeader.getAlgorithm().isSetBLOCK()
                || !bloomFilterHeader.getCompression().isSetUNCOMPRESSED()) {
            return Optional.empty();
        }

        byte[] bitset = new byte[numBytes];
        seekableInputStream.readFully(bitset);
        Optional<BloomFilter> returnValue = Optional.of(new BlockSplitBloomFilter(bitset));
        Instant end = Instant.now();
        LOG.info("Total Time taken to construct bloom filter : " + Duration.between(start,end).toMillis());
        return returnValue;
    }
}
