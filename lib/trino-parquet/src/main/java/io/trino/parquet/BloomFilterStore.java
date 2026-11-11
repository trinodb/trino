/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.trino.parquet.crypto.AesCipherUtils;
import io.trino.parquet.crypto.ColumnDecryptionContext;
import io.trino.parquet.crypto.FileDecryptionContext;
import io.trino.parquet.crypto.ModuleType;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter.UPPER_BOUND_BYTES;

public class BloomFilterStore
{
    // since bloomfilter header is relatively small(18 bytes when testing) we can read in a larger buffer(BlockSplitBloomFilter.HEADER_SIZE*4 in this case)
    // and get actual bytes used when deserializing header in order to calculate the correct offset for bloomfilter data.
    private static final int MAX_HEADER_LENGTH = BlockSplitBloomFilter.HEADER_SIZE * 4;

    private final ParquetDataSource dataSource;
    private final Map<ColumnPath, ColumnChunkMetadata> columnChunks;
    private final Optional<FileDecryptionContext> decryptionContext;

    public BloomFilterStore(ParquetDataSource dataSource, BlockMetadata block, Set<ColumnPath> columnsFiltered, Optional<FileDecryptionContext> decryptionContext)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        requireNonNull(block, "block is null");
        requireNonNull(columnsFiltered, "columnsFiltered is null");
        this.decryptionContext = requireNonNull(decryptionContext, "decryptionContext is null");

        ImmutableMap.Builder<ColumnPath, ColumnChunkMetadata> chunkBuilder = ImmutableMap.builder();
        for (ColumnChunkMetadata column : block.columns()) {
            ColumnPath path = column.getPath();
            if (hasBloomFilter(column) && columnsFiltered.contains(path)) {
                chunkBuilder.put(path, column);
            }
        }
        this.columnChunks = chunkBuilder.buildOrThrow();
    }

    public Optional<BloomFilter> getBloomFilter(ColumnPath columnPath)
    {
        BloomFilterHeader bloomFilterHeader;
        long bloomFilterDataOffset;
        // Bytes following the header which were fetched together with it
        Slice fetchedBitset = EMPTY_SLICE;
        ColumnChunkMetadata chunk = columnChunks.get(columnPath);
        if (chunk == null) {
            return Optional.empty();
        }
        long columnBloomFilterOffset = chunk.getBloomFilterOffset();
        try {
            // If the column is encrypted, decrypt the header using the metadata decryptor
            Optional<ColumnDecryptionContext> columnContext = decryptionContext.flatMap(context -> context.getColumnDecryptionContext(columnPath));
            if (columnContext.isPresent()) {
                // Read encrypted header module: SIZE(4) + NONCE + CIPHERTEXT + TAG
                int encryptedSize = BytesUtils.readIntLittleEndian(dataSource.readFully(columnBloomFilterOffset, AesCipherUtils.SIZE_LENGTH).getBytes(), 0);
                Slice module = dataSource.readFully(columnBloomFilterOffset, AesCipherUtils.SIZE_LENGTH + encryptedSize);
                BasicSliceInput in = module.getInput();
                byte[] aad = AesCipherUtils.createModuleAAD(columnContext.get().fileAad(), ModuleType.BloomFilterHeader, chunk.getRowGroupOrdinal(), chunk.getColumnOrdinal(), -1);
                bloomFilterHeader = Util.readBloomFilterHeader(in, columnContext.get().metadataDecryptor(), aad);
                // after read, position() == 4 + encrypted data length
                bloomFilterDataOffset = columnBloomFilterOffset + in.position();
            }
            else {
                // The header and the bitset are fetched with a single read when the writer recorded their total size
                int recordedLength = chunk.getBloomFilterLength();
                int readLength = MAX_HEADER_LENGTH;
                if (recordedLength > MAX_HEADER_LENGTH && recordedLength <= MAX_HEADER_LENGTH + UPPER_BOUND_BYTES) {
                    readLength = recordedLength;
                }
                Slice bloomFilter = dataSource.readFully(columnBloomFilterOffset, readLength);
                BasicSliceInput headerSliceInput = bloomFilter.getInput();
                bloomFilterHeader = Util.readBloomFilterHeader(headerSliceInput);
                int headerLength = toIntExact(headerSliceInput.position());
                bloomFilterDataOffset = columnBloomFilterOffset + headerLength;
                fetchedBitset = bloomFilter.slice(headerLength, readLength - headerLength);
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read Bloom filter header", exception);
        }

        if (!bloomFilterSupported(columnPath, bloomFilterHeader)) {
            return Optional.empty();
        }

        try {
            Optional<ColumnDecryptionContext> columnContext = decryptionContext.flatMap(context -> context.getColumnDecryptionContext(columnPath));
            if (columnContext.isPresent()) {
                // Read the whole bitset module: SIZE + NONCE + CIPHERTEXT + TAG
                int encryptedSize = BytesUtils.readIntLittleEndian(dataSource.readFully(bloomFilterDataOffset, AesCipherUtils.SIZE_LENGTH).getBytes(), 0);
                Slice module = dataSource.readFully(bloomFilterDataOffset, AesCipherUtils.SIZE_LENGTH + encryptedSize);
                byte[] aad = AesCipherUtils.createModuleAAD(columnContext.get().fileAad(), ModuleType.BloomFilterBitset, chunk.getRowGroupOrdinal(), chunk.getColumnOrdinal(), -1);
                byte[] plain = columnContext.get().metadataDecryptor().decrypt(module.getBytes(), aad);
                verify(plain.length == bloomFilterHeader.getNumBytes(), "Decrypted bloom filter length mismatch: expected %s, got %s", bloomFilterHeader.getNumBytes(), plain.length);
                return Optional.of(new BlockSplitBloomFilter(plain));
            }
            else {
                // Plaintext bitset
                if (bloomFilterHeader.getNumBytes() <= fetchedBitset.length()) {
                    return Optional.of(new BlockSplitBloomFilter(fetchedBitset.getBytes(0, bloomFilterHeader.getNumBytes())));
                }
                Slice bloomFilterData = dataSource.readFully(bloomFilterDataOffset, bloomFilterHeader.getNumBytes());
                verify(bloomFilterData.length() > 0, "Read empty bloom filter %s", bloomFilterHeader);
                return Optional.of(new BlockSplitBloomFilter(bloomFilterData.getBytes()));
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Failed to read Bloom filter data", exception);
        }
    }

    public static Optional<BloomFilterStore> getBloomFilterStore(
            ParquetDataSource dataSource,
            BlockMetadata blockMetadata,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            ParquetReaderOptions options,
            Optional<FileDecryptionContext> decryptionContext)
    {
        if (!options.useBloomFilter() || parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return Optional.empty();
        }

        boolean hasBloomFilter = blockMetadata.columns().stream().anyMatch(BloomFilterStore::hasBloomFilter);
        if (!hasBloomFilter) {
            return Optional.empty();
        }

        Map<ColumnDescriptor, Domain> parquetDomains = parquetTupleDomain.getDomains()
                .orElseThrow(() -> new IllegalStateException("Predicate other than none should have domains"));
        Set<ColumnPath> columnsFilteredPaths = parquetDomains.keySet().stream()
                .map(column -> ColumnPath.get(column.getPath()))
                .collect(toImmutableSet());

        return Optional.of(new BloomFilterStore(dataSource, blockMetadata, columnsFilteredPaths, decryptionContext));
    }

    public static boolean hasBloomFilter(ColumnChunkMetadata columnMetaData)
    {
        return columnMetaData.getBloomFilterOffset() > 0;
    }

    private static boolean bloomFilterSupported(ColumnPath columnPath, BloomFilterHeader bloomFilterHeader)
    {
        int numBytes = bloomFilterHeader.getNumBytes();
        if (numBytes <= 0 || numBytes > UPPER_BOUND_BYTES) {
            throw new ParquetDecodingException(format("Column: %s has bloom filter number of bytes value of %d, which is out of bound of lower limit: %d and upper limit: %d", columnPath, numBytes, 0, UPPER_BOUND_BYTES));
        }
        return bloomFilterHeader.getHash().isSetXXHASH() && bloomFilterHeader.getAlgorithm().isSetBLOCK() && bloomFilterHeader.getCompression().isSetUNCOMPRESSED();
    }
}
