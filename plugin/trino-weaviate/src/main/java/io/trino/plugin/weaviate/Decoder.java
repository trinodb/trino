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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.PhoneNumber;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.weaviate.WeaviateColumnHandle.GEO_COORDINATES;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.PHONE_NUMBER;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_UNSUPPORTED_DATA_TYPE;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;

public final class Decoder
{
    private Decoder() {}

    public static void decode(BlockBuilder blockBuilder, Object raw, Type type)
    {
        switch (type) {
            case ArrayType arrayType -> decodeArray(blockBuilder, raw, arrayType);
            case RowType rowType -> decodeRow(blockBuilder, raw, rowType);
            default -> decodePrimitive(blockBuilder, type, raw);
        }
    }

    private static void decodePrimitive(BlockBuilder blockBuilder, Type type, Object raw)
    {
        if (raw == null) {
            requireNonNull(blockBuilder, "blockBuilder is null");
            blockBuilder.appendNull();
            return;
        }
        switch (type) {
            case VarcharType t -> t.writeString(blockBuilder, (String) raw);
            case BooleanType t -> t.writeBoolean(blockBuilder, (Boolean) raw);
            case DoubleType t -> t.writeDouble(blockBuilder, ((Number) raw).doubleValue());
            case IntegerType t -> t.writeLong(blockBuilder, ((Number) raw).longValue());
            case TimestampType _ -> TIMESTAMP_MILLIS.writeLong(blockBuilder, ((OffsetDateTime) raw).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND);
            default -> throw new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, type + " is not supported");
        }
    }

    private static void decodeArray(BlockBuilder parentBlockBuilder, Object raw, ArrayType arrayType)
    {
        if (raw == null) {
            requireNonNull(parentBlockBuilder, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return;
        }

        List<?> list = (List<?>) raw;
        Type elementType = arrayType.getElementType();

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            decode(blockBuilder, element, elementType);
        }

        Block block = blockBuilder.build();
        if (parentBlockBuilder != null) {
            arrayType.writeObject(parentBlockBuilder, block);
        }
    }

    private static void decodeRow(BlockBuilder blockBuilder, Object raw, RowType rowType)
    {
        if (raw == null) {
            requireNonNull(blockBuilder, "blockBuilder is null");
            blockBuilder.appendNull();
            return;
        }

        Map<String, Object> row;
        if (rowType.equals(GEO_COORDINATES)) {
            row = geoCoordinatesMap((GeoCoordinates) raw);
        }
        else if (rowType.equals(PHONE_NUMBER)) {
            row = phoneNumberMap((PhoneNumber) raw);
        }
        else {
            row = (Map<String, Object>) raw;
        }

        if (blockBuilder == null) {
            buildRowValue(rowType, fieldBuilders -> buildRow(rowType, row, fieldBuilders));
            return;
        }

        RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) blockBuilder;
        rowBlockBuilder.buildEntry(fieldBuilders -> buildRow(rowType, row, fieldBuilders));
    }

    private static void buildRow(RowType type, Map<String, Object> row, List<BlockBuilder> fieldBuilders)
    {
        List<RowType.Field> fields = type.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String fieldName = field.getName()
                    .orElseThrow(() -> WeaviateErrorCode.typeNotSupported("unnamed ROW values"));
            decode(fieldBuilders.get(i), row.get(fieldName), field.getType());
        }
    }

    private static Map<String, Object> geoCoordinatesMap(GeoCoordinates geoCoordinates)
    {
        List<RowType.Field> fields = ((RowType) GEO_COORDINATES).getFields();

        String latitude = fields.getFirst().getName().orElseThrow();
        String longitude = fields.getLast().getName().orElseThrow();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        appendMapEntry(builder, latitude, geoCoordinates::latitude);
        appendMapEntry(builder, longitude, geoCoordinates::longitude);
        return builder.buildOrThrow();
    }

    private static Map<String, Object> phoneNumberMap(PhoneNumber phoneNumber)
    {
        List<RowType.Field> fields = ((RowType) PHONE_NUMBER).getFields();

        String defaultCountry = fields.get(0).getName().orElseThrow();
        String countryCode = fields.get(1).getName().orElseThrow();
        String internationalFormatted = fields.get(2).getName().orElseThrow();
        String national = fields.get(3).getName().orElseThrow();
        String nationalFormatted = fields.get(4).getName().orElseThrow();
        String valid = fields.get(5).getName().orElseThrow();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        appendMapEntry(builder, defaultCountry, phoneNumber::defaultCountry);
        appendMapEntry(builder, countryCode, phoneNumber::countryCode);
        appendMapEntry(builder, internationalFormatted, phoneNumber::internationalFormatted);
        appendMapEntry(builder, national, phoneNumber::national);
        appendMapEntry(builder, nationalFormatted, phoneNumber::nationalFormatted);
        appendMapEntry(builder, valid, phoneNumber::valid);
        return builder.buildOrThrow();
    }

    private static void appendMapEntry(ImmutableMap.Builder<String, Object> builder, String key, Supplier<Object> supplier)
    {
        Object value = supplier.get();
        if (value == null) {
            return;
        }
        builder.put(key, value);
    }
}
