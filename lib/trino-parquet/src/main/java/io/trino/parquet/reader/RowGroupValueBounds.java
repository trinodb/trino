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

import io.trino.parquet.Column;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.List;
import java.util.Optional;

import static io.trino.parquet.predicate.TupleDomainParquetPredicate.getDomain;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.joda.time.DateTimeZone.UTC;

/**
 * The values that the columns of one row group are known to hold, taken from the row group statistics. Every page
 * read from the row group shares the instance, and each column is resolved once, when first asked for.
 * <p>
 * Only top level integer and date columns are reported. Their statistics have one ordering, no value outside it
 * and no dependency on a time zone.
 */
final class RowGroupValueBounds
{
    private final List<Column> columns;
    private final PrunedBlockMetadata blockMetadata;
    private final ParquetDataSourceId dataSourceId;
    private final Optional<Domain>[] bounds;

    @SuppressWarnings("unchecked")
    RowGroupValueBounds(List<Column> columns, PrunedBlockMetadata blockMetadata, ParquetDataSourceId dataSourceId)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.blockMetadata = requireNonNull(blockMetadata, "blockMetadata is null");
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.bounds = (Optional<Domain>[]) new Optional<?>[columns.size()];
    }

    Optional<Domain> valueBounds(int channel)
    {
        if (channel >= bounds.length) {
            // the row number column
            return Optional.empty();
        }
        Optional<Domain> channelBounds = bounds[channel];
        if (channelBounds == null) {
            channelBounds = resolve(channel);
            bounds[channel] = channelBounds;
        }
        return channelBounds;
    }

    private Optional<Domain> resolve(int channel)
    {
        if (!(columns.get(channel).field() instanceof PrimitiveField field) || !isSupported(field)) {
            return Optional.empty();
        }
        try {
            ColumnDescriptor descriptor = field.getDescriptor();
            ColumnChunkMetadata metadata = blockMetadata.getColumnChunkMetaData(descriptor);
            Domain domain = getDomain(descriptor, field.getType(), metadata.getValueCount(), metadata.getStatistics(), dataSourceId, UTC);
            if (domain.isAll()) {
                return Optional.empty();
            }
            return Optional.of(domain);
        }
        catch (ParquetCorruptionException e) {
            // unusable statistics tell nothing about the values
            return Optional.empty();
        }
    }

    private static boolean isSupported(PrimitiveField field)
    {
        ColumnDescriptor descriptor = field.getDescriptor();
        if (descriptor.getPath().length != 1 || descriptor.getMaxRepetitionLevel() != 0) {
            return false;
        }
        LogicalTypeAnnotation annotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
        boolean signedIntegerOrDate = annotation == null
                || annotation instanceof DateLogicalTypeAnnotation
                || (annotation instanceof IntLogicalTypeAnnotation integer && integer.isSigned());
        if (!signedIntegerOrDate) {
            return false;
        }
        Type type = field.getType();
        PrimitiveTypeName physicalType = descriptor.getPrimitiveType().getPrimitiveTypeName();
        if (physicalType == INT64) {
            return type.equals(BIGINT);
        }
        if (physicalType == INT32) {
            return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) || type.equals(DATE);
        }
        return false;
    }
}
