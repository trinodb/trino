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
package io.trino.parquet.writer;

import io.trino.parquet.ParquetMetadataConverter;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.io.api.Binary;

import static com.google.common.base.Verify.verify;
import static io.trino.parquet.ParquetMetadataConverter.MAX_STATS_SIZE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;

public final class ParquetMetadataUtils
{
    private ParquetMetadataUtils() {}

    public static <T extends Comparable<T>> Statistics toParquetStatistics(org.apache.parquet.column.statistics.Statistics<T> stats, int truncateLength)
    {
        // TODO Utilize https://github.com/apache/parquet-format/pull/216 when available to populate is_max_value_exact/is_min_value_exact
        if (isTruncationPossible(stats, truncateLength)) {
            // parquet-mr drops statistics larger than MAX_STATS_SIZE rather than truncating them.
            // In order to ensure truncation rather than no stats, we need to use a truncateLength which would never exceed ParquetMetadataConverter.MAX_STATS_SIZE
            verify(
                    2L * truncateLength < MAX_STATS_SIZE,
                    "Twice of truncateLength %s must be less than MAX_STATS_SIZE %s",
                    truncateLength,
                    MAX_STATS_SIZE);
            // We need to take a lock here because CharsetValidator inside BinaryTruncator modifies a reusable dummyBuffer in-place
            // and DEFAULT_UTF8_TRUNCATOR is a static instance, which makes this method thread unsafe.
            // isTruncationPossible should ensure that locking is used only when we expect truncation, which is an uncommon scenario.
            // TODO remove synchronization when we use a release with the fix https://github.com/apache/parquet-mr/pull/1154
            synchronized (ParquetMetadataUtils.class) {
                return ParquetMetadataConverter.toParquetStatistics(stats, truncateLength);
            }
        }
        return ParquetMetadataConverter.toParquetStatistics(stats);
    }

    private static <T extends Comparable<T>> boolean isTruncationPossible(org.apache.parquet.column.statistics.Statistics<T> stats, int truncateLength)
    {
        PrimitiveTypeName primitiveType = stats.type().getPrimitiveTypeName();
        if (!primitiveType.equals(BINARY) && !primitiveType.equals(FIXED_LEN_BYTE_ARRAY)) {
            return false;
        }
        if (stats.isEmpty() || !stats.hasNonNullValue() || !(stats instanceof BinaryStatistics binaryStatistics)) {
            return false;
        }
        // non-null value exists, so min and max can't be null
        Binary min = binaryStatistics.genericGetMin();
        Binary max = binaryStatistics.genericGetMax();
        return min.length() > truncateLength || max.length() > truncateLength;
    }
}
