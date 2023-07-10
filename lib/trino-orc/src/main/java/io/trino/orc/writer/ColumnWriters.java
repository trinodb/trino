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
package io.trino.orc.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.statistics.BinaryStatisticsBuilder;
import io.trino.orc.metadata.statistics.BloomFilterBuilder;
import io.trino.orc.metadata.statistics.DateStatisticsBuilder;
import io.trino.orc.metadata.statistics.DoubleStatisticsBuilder;
import io.trino.orc.metadata.statistics.IntegerStatisticsBuilder;
import io.trino.orc.metadata.statistics.StringStatisticsBuilder;
import io.trino.orc.metadata.statistics.TimestampStatisticsBuilder;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class ColumnWriters
{
    private ColumnWriters() {}

    public static ColumnWriter createColumnWriter(
            OrcColumnId columnId,
            ColumnMetadata<OrcType> orcTypes,
            Type type,
            CompressionKind compression,
            int bufferSize,
            DataSize stringStatisticsLimit,
            Supplier<BloomFilterBuilder> bloomFilterBuilder,
            boolean shouldCompactMinMax)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(columnId);
        if (type instanceof TimeType timeType) {
            checkArgument(timeType.getPrecision() == 6, "%s not supported for ORC writer", type);
            checkArgument(orcType.getOrcTypeKind() == LONG, "wrong ORC type %s for type %s", orcType, type);
            checkArgument("TIME".equals(orcType.getAttributes().get("iceberg.long-type")), "wrong attributes %s for type %s", orcType.getAttributes(), type);
            return new TimeColumnWriter(columnId, type, compression, bufferSize, () -> new IntegerStatisticsBuilder(bloomFilterBuilder.get()));
        }
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(columnId, type, compression, bufferSize);

            case FLOAT:
                return new FloatColumnWriter(columnId, type, compression, bufferSize, () -> new DoubleStatisticsBuilder(bloomFilterBuilder.get()));

            case DOUBLE:
                return new DoubleColumnWriter(columnId, type, compression, bufferSize, () -> new DoubleStatisticsBuilder(bloomFilterBuilder.get()));

            case BYTE:
                return new ByteColumnWriter(columnId, type, compression, bufferSize);

            case DATE:
                return new LongColumnWriter(columnId, type, compression, bufferSize, () -> new DateStatisticsBuilder(bloomFilterBuilder.get()));

            case SHORT:
            case INT:
            case LONG:
                return new LongColumnWriter(columnId, type, compression, bufferSize, () -> new IntegerStatisticsBuilder(bloomFilterBuilder.get()));

            case DECIMAL:
                return new DecimalColumnWriter(columnId, type, compression, bufferSize);

            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new TimestampColumnWriter(columnId, type, compression, bufferSize, () -> new TimestampStatisticsBuilder(bloomFilterBuilder.get()));

            case BINARY:
                return new SliceDirectColumnWriter(columnId, type, compression, bufferSize, BinaryStatisticsBuilder::new);

            case CHAR:
            case VARCHAR:
            case STRING:
                return new SliceDictionaryColumnWriter(columnId, type, compression, bufferSize, () -> new StringStatisticsBuilder(toIntExact(stringStatisticsLimit.toBytes()), bloomFilterBuilder.get(), shouldCompactMinMax));

            case LIST: {
                OrcColumnId fieldColumnIndex = orcType.getFieldTypeIndex(0);
                Type fieldType = type.getTypeParameters().get(0);
                ColumnWriter elementWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder, shouldCompactMinMax);
                return new ListColumnWriter(columnId, compression, bufferSize, elementWriter);
            }

            case MAP: {
                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        orcTypes,
                        type.getTypeParameters().get(0),
                        compression,
                        bufferSize,
                        stringStatisticsLimit,
                        bloomFilterBuilder,
                        shouldCompactMinMax);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        orcTypes,
                        type.getTypeParameters().get(1),
                        compression,
                        bufferSize,
                        stringStatisticsLimit,
                        bloomFilterBuilder,
                        shouldCompactMinMax);
                return new MapColumnWriter(columnId, compression, bufferSize, keyWriter, valueWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    OrcColumnId fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    Type fieldType = type.getTypeParameters().get(fieldId);
                    fieldWriters.add(createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, stringStatisticsLimit, bloomFilterBuilder, shouldCompactMinMax));
                }
                return new StructColumnWriter(columnId, compression, bufferSize, fieldWriters.build());
            }

            case UNION:
                // unsupported
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
