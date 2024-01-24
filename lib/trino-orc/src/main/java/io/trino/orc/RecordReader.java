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
package io.trino.orc;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.metadata.statistics.StripeStatistics;
import io.trino.orc.reader.ColumnReader;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.orc.reader.ColumnReaders.createColumnReader;
import static java.util.Objects.requireNonNull;

public interface RecordReader
        extends Closeable
{
    ColumnMetadata<OrcType> getColumnTypes();

    Page nextPage()
            throws IOException;

    long getFilePosition();

    long getMaxCombinedBytesPerRow();

    long getTotalDataLength();

    @Override
    void close()
            throws IOException;

    Optional<Long> getStartRowPosition();

    Optional<Long> getEndRowPosition();

    default boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    default boolean isStripeIncluded(
            StripeInformation stripe,
            Optional<StripeStatistics> stripeStats,
            OrcPredicate predicate)
    {
        // if there are no stats, include the column
        return stripeStats
                .map(StripeStatistics::getColumnStatistics)
                .map(columnStats -> predicate.matches(stripe.getNumberOfRows(), columnStats))
                .orElse(true);
    }

    default ColumnReader[] createColumnReaders(
            List<OrcColumn> columns,
            List<Type> readTypes,
            List<OrcReader.ProjectedLayout> readLayouts,
            AggregatedMemoryContext memoryContext,
            OrcBlockFactory blockFactory,
            OrcReader.FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        ColumnReader[] columnReaders = new ColumnReader[columns.size()];
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            Type readType = readTypes.get(columnIndex);
            OrcColumn column = columns.get(columnIndex);
            OrcReader.ProjectedLayout projectedLayout = readLayouts.get(columnIndex);
            columnReaders[columnIndex] = createColumnReader(readType, column, projectedLayout, memoryContext, blockFactory, fieldMapperFactory);
        }
        return columnReaders;
    }

    class StripeInfo
    {
        private final StripeInformation stripe;
        private final Optional<StripeStatistics> stats;

        public StripeInfo(StripeInformation stripe, Optional<StripeStatistics> stats)
        {
            this.stripe = requireNonNull(stripe, "stripe is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        public StripeInformation getStripe()
        {
            return stripe;
        }

        public Optional<StripeStatistics> getStats()
        {
            return stats;
        }
    }
}
