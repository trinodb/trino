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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

import static io.trino.parquet.ParquetMetadataConverter.fromParquetStatistics;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.parquet.format.Util.readFileMetaData;

public final class MetadataReader
{
    private static final Slice MAGIC = Slices.utf8Slice("PAR1");
    private static final int POST_SCRIPT_SIZE = Integer.BYTES + MAGIC.length();
    // Typical 1GB files produced by Trino were found to have footer size between 30-40KB
    private static final int EXPECTED_FOOTER_SIZE = 48 * 1024;

    private MetadataReader() {}

    public static ParquetMetadata readFooter(ParquetDataSource dataSource, Optional<ParquetWriteValidation> parquetWriteValidation)
            throws IOException
    {
        // Parquet File Layout:
        //
        // MAGIC
        // variable: Data
        // variable: Metadata
        // 4 bytes: MetadataLength
        // MAGIC

        validateParquet(dataSource.getEstimatedSize() >= MAGIC.length() + POST_SCRIPT_SIZE, dataSource.getId(), "%s is not a valid Parquet File", dataSource.getId());

        // Read the tail of the file
        long estimatedFileSize = dataSource.getEstimatedSize();
        long expectedReadSize = min(estimatedFileSize, EXPECTED_FOOTER_SIZE);
        Slice buffer = dataSource.readTail(toIntExact(expectedReadSize));

        Slice magic = buffer.slice(buffer.length() - MAGIC.length(), MAGIC.length());
        validateParquet(MAGIC.equals(magic), dataSource.getId(), "Expected magic number: %s got: %s", MAGIC.toStringUtf8(), magic.toStringUtf8());

        int metadataLength = buffer.getInt(buffer.length() - POST_SCRIPT_SIZE);
        long metadataIndex = estimatedFileSize - POST_SCRIPT_SIZE - metadataLength;
        validateParquet(
                metadataIndex >= MAGIC.length() && metadataIndex < estimatedFileSize - POST_SCRIPT_SIZE,
                dataSource.getId(),
                "Metadata index: %s out of range",
                metadataIndex);

        int completeFooterSize = metadataLength + POST_SCRIPT_SIZE;
        if (completeFooterSize > buffer.length()) {
            // initial read was not large enough, so just read again with the correct size
            buffer = dataSource.readTail(completeFooterSize);
        }
        InputStream metadataStream = buffer.slice(buffer.length() - completeFooterSize, metadataLength).getInput();

        FileMetaData fileMetaData = readFileMetaData(metadataStream);
        ParquetMetadata parquetMetadata = new ParquetMetadata(fileMetaData, dataSource.getId());
        validateFileMetadata(dataSource.getId(), parquetMetadata.getFileMetaData(), parquetWriteValidation);
        return parquetMetadata;
    }

    public static org.apache.parquet.column.statistics.Statistics<?> readStats(Optional<String> fileCreatedBy, Optional<Statistics> statisticsFromFile, PrimitiveType type)
    {
        Statistics statistics = statisticsFromFile.orElse(null);
        org.apache.parquet.column.statistics.Statistics<?> columnStatistics = fromParquetStatistics(fileCreatedBy.orElse(null), statistics, type);

        if (isStringType(type)
                && statistics != null
                && !statistics.isSetMin_value() && !statistics.isSetMax_value() // the min,max fields used for UTF8 since Parquet PARQUET-1025
                && statistics.isSetMin() && statistics.isSetMax()  // the min,max fields used for UTF8 before Parquet PARQUET-1025
                && columnStatistics.genericGetMin() == null && columnStatistics.genericGetMax() == null
                && !CorruptStatistics.shouldIgnoreStatistics(fileCreatedBy.orElse(null), type.getPrimitiveTypeName())) {
            columnStatistics = tryReadOldUtf8Stats(statistics, (BinaryStatistics) columnStatistics);
        }

        return columnStatistics;
    }

    private static boolean isStringType(PrimitiveType type)
    {
        if (type.getLogicalTypeAnnotation() == null) {
            return false;
        }

        return type.getLogicalTypeAnnotation()
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Boolean>()
                {
                    @Override
                    public Optional<Boolean> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType)
                    {
                        return Optional.of(TRUE);
                    }
                })
                .orElse(FALSE);
    }

    private static org.apache.parquet.column.statistics.Statistics<?> tryReadOldUtf8Stats(Statistics statistics, BinaryStatistics columnStatistics)
    {
        byte[] min = statistics.getMin();
        byte[] max = statistics.getMax();

        if (Arrays.equals(min, max)) {
            // If min=max, then there is single value only
            min = min.clone();
            max = min;
        }
        else {
            int commonPrefix = commonPrefix(min, max);

            // For min we can retain all-ASCII, because this produces a strictly lower value.
            int minGoodLength = commonPrefix;
            while (minGoodLength < min.length && isAscii(min[minGoodLength])) {
                minGoodLength++;
            }

            // For max we can be sure only of the part matching the min. When they differ, we can consider only one next, and only if both are ASCII
            int maxGoodLength = commonPrefix;
            if (maxGoodLength < max.length && maxGoodLength < min.length && isAscii(min[maxGoodLength]) && isAscii(max[maxGoodLength])) {
                maxGoodLength++;
            }
            // Incrementing 127 would overflow. Incrementing within non-ASCII can have side-effects.
            while (maxGoodLength > 0 && (max[maxGoodLength - 1] == 127 || !isAscii(max[maxGoodLength - 1]))) {
                maxGoodLength--;
            }
            if (maxGoodLength == 0) {
                // We can return just min bound, but code downstream likely expects both are present or both are absent.
                return columnStatistics;
            }

            min = Arrays.copyOf(min, minGoodLength);
            max = Arrays.copyOf(max, maxGoodLength);
            max[maxGoodLength - 1]++;
        }

        return org.apache.parquet.column.statistics.Statistics
                .getBuilderForReading(columnStatistics.type())
                       .withMin(min)
                       .withMax(max)
                       .withNumNulls(!columnStatistics.isNumNullsSet() && statistics.isSetNull_count() ? statistics.getNull_count() : columnStatistics.getNumNulls())
                       .build();
    }

    private static boolean isAscii(byte b)
    {
        return 0 <= b;
    }

    private static int commonPrefix(byte[] a, byte[] b)
    {
        int commonPrefixLength = 0;
        while (commonPrefixLength < a.length && commonPrefixLength < b.length && a[commonPrefixLength] == b[commonPrefixLength]) {
            commonPrefixLength++;
        }
        return commonPrefixLength;
    }

    private static void validateFileMetadata(ParquetDataSourceId dataSourceId, FileMetadata fileMetaData, Optional<ParquetWriteValidation> parquetWriteValidation)
            throws ParquetCorruptionException
    {
        if (parquetWriteValidation.isEmpty()) {
            return;
        }
        ParquetWriteValidation writeValidation = parquetWriteValidation.get();
        writeValidation.validateTimeZone(
                dataSourceId,
                Optional.ofNullable(fileMetaData.getKeyValueMetaData().get("writer.time.zone")));
        writeValidation.validateColumns(dataSourceId, fileMetaData.getSchema());
    }
}
