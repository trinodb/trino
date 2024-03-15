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
package io.trino.plugin.deltalake.kernel.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.trino.plugin.deltalake.kernel.KernelSchemaUtils;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.sql.Date;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.plugin.deltalake.kernel.data.DataUtils.createBlockBuilder;
import static java.lang.String.format;

public class JsonRowParser
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Optional<boolean[]> NULL_BLOCK = Optional.of(new boolean[] {true});
    private static final Optional<boolean[]> NOT_NULL_BLOCK = Optional.of(new boolean[] {false});

    private static final ValueBlock NULL_BYTE_BLOCK = new ByteArrayBlock(1, NULL_BLOCK, new byte[1]);
    private static final ValueBlock NULL_INT_BLOCK = new IntArrayBlock(1, NOT_NULL_BLOCK, new int[1]);
    private static final ValueBlock NULL_LONG_BLOCK = new LongArrayBlock(1, NOT_NULL_BLOCK, new long[1]);
    private static final ValueBlock NULL_VARWIDTH_BLOCK = new VariableWidthBlock(1, EMPTY_SLICE, new int[] {0, 0}, NULL_BLOCK);

    private JsonRowParser()
    {
    }

    public static Page parseJsonRow(String json, StructType readSchema, TypeManager typeManager)
    {
        try {
            final JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
            Block[] blocks = new Block[readSchema.length()];
            for (int i = 0; i < readSchema.length(); i++) {
                final StructField field = readSchema.at(i);
                final Block block = decodeField(typeManager, (ObjectNode) jsonNode, field);
                blocks[i] = block;
            }
            return new Page(1, blocks);
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(format("Could not parse JSON: %s", json), ex);
        }
        catch (Throwable t) {
            // debugging only
            throw t;
        }
    }

    private static ValueBlock decodeElement(TypeManager typeManager, JsonNode jsonValue, DataType dataType)
    {
        if (dataType instanceof BooleanType) {
            throwIfTypeMismatch("boolean", jsonValue.isBoolean(), jsonValue);
            if (jsonValue.isNull()) {
                return NULL_BYTE_BLOCK;
            }
            return new ByteArrayBlock(1, NOT_NULL_BLOCK, new byte[] {(byte) (jsonValue.booleanValue() ? 1 : 0)});
        }

        if (dataType instanceof ByteType) {
            throwIfTypeMismatch(
                    "byte",
                    jsonValue.canConvertToExactIntegral()
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() <= Byte.MAX_VALUE
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() >= Byte.MIN_VALUE,
                    jsonValue);

            if (jsonValue.isNull()) {
                return NULL_BYTE_BLOCK;
            }
            else {
                return new ByteArrayBlock(1, NOT_NULL_BLOCK, new byte[] {(byte) jsonValue.intValue()});
            }
        }

        if (dataType instanceof ShortType) {
            throwIfTypeMismatch(
                    "short",
                    jsonValue.canConvertToExactIntegral()
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() <= Short.MAX_VALUE
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() >= Short.MIN_VALUE,
                    jsonValue);

            if (jsonValue.isNull()) {
                return NULL_INT_BLOCK;
            }
            else {
                return new IntArrayBlock(1, NOT_NULL_BLOCK, new int[] {jsonValue.intValue()});
            }
        }

        if (dataType instanceof IntegerType) {
            throwIfTypeMismatch(
                    "integer", jsonValue.isIntegralNumber() && jsonValue.canConvertToInt(), jsonValue);

            if (jsonValue.isNull()) {
                return NULL_INT_BLOCK;
            }
            else {
                return new IntArrayBlock(1, NOT_NULL_BLOCK, new int[] {jsonValue.intValue()});
            }
        }

        if (dataType instanceof LongType) {
            throwIfTypeMismatch(
                    "long", jsonValue.isIntegralNumber() && jsonValue.canConvertToLong(), jsonValue);

            if (jsonValue.isNull()) {
                return NULL_LONG_BLOCK;
            }
            else {
                return new LongArrayBlock(1, NOT_NULL_BLOCK, new long[] {jsonValue.longValue()});
            }
        }

        if (dataType instanceof FloatType) {
            float value = 0;
            switch (jsonValue.getNodeType()) {
                case NUMBER:
                    throwIfTypeMismatch(
                            "float",
                            // floatValue() will be converted to +/-INF if it cannot be represented
                            // by a float
                            // Note it is still possible to lose precision in this conversion but
                            // checking for that requires converting to a float and back to BigDecimal
                            !Float.isInfinite(jsonValue.floatValue()),
                            jsonValue);
                    value = jsonValue.floatValue();
                    break;
                case STRING:
                    switch (jsonValue.asText()) {
                        case "NaN":
                            value = Float.NaN;
                            break;
                        case "+INF":
                        case "+Infinity":
                        case "Infinity":
                            value = Float.POSITIVE_INFINITY;
                            break;
                        case "-INF":
                        case "-Infinity":
                            value = Float.NEGATIVE_INFINITY;
                            break;
                    }
                default:
                    throwIfTypeMismatch("float", false, jsonValue);
            }

            if (jsonValue.isNull()) {
                return NULL_INT_BLOCK;
            }
            else {
                return new IntArrayBlock(1, NOT_NULL_BLOCK, new int[] {Float.floatToRawIntBits(value)});
            }
        }

        if (dataType instanceof DoubleType) {
            double value = 0;
            switch (jsonValue.getNodeType()) {
                case NUMBER:
                    throwIfTypeMismatch(
                            "double",
                            // doubleValue() will be converted to +/-INF if it cannot be represented by
                            // a double
                            // Note it is still possible to lose precision in this conversion but
                            // checking for that requires converting to a double and back to BigDecimal
                            !Double.isInfinite(jsonValue.doubleValue()),
                            jsonValue);
                    value = jsonValue.doubleValue();
                    break;
                case STRING:
                    switch (jsonValue.asText()) {
                        case "NaN":
                            value = Double.NaN;
                            break;
                        case "+INF":
                        case "+Infinity":
                        case "Infinity":
                            value = Double.POSITIVE_INFINITY;
                            break;
                        case "-INF":
                        case "-Infinity":
                            value = Double.NEGATIVE_INFINITY;
                            break;
                    }
                default:
                    throwIfTypeMismatch("double", false, jsonValue);
            }

            if (jsonValue.isNull()) {
                return NULL_LONG_BLOCK;
            }
            else {
                return new LongArrayBlock(1, NOT_NULL_BLOCK, new long[] {Double.doubleToRawLongBits(value)});
            }
        }

        if (dataType instanceof StringType) {
            throwIfTypeMismatch("string", jsonValue.isTextual(), jsonValue);
            if (jsonValue.isNull()) {
                return NULL_VARWIDTH_BLOCK;
            }
            else {
                Slice slice = Slices.utf8Slice(jsonValue.textValue());
                return new VariableWidthBlock(1, slice, new int[] {0, slice.length()}, NOT_NULL_BLOCK);
            }
        }

        if (dataType instanceof DecimalType) {
            throwIfTypeMismatch("decimal", jsonValue.isNumber(), jsonValue);
            throw new UnsupportedOperationException("not yet supported");
        }

        if (dataType instanceof DateType) {
            throwIfTypeMismatch("date", jsonValue.isTextual(), jsonValue);
            if (jsonValue.isNull()) {
                return NULL_INT_BLOCK;
            }
            else {
                int daysSinceEpoch = InternalUtils.daysSinceEpoch(Date.valueOf(jsonValue.textValue()));
                return new IntArrayBlock(1, NOT_NULL_BLOCK, new int[] {daysSinceEpoch});
            }
        }

        if (dataType instanceof TimestampType) {
            throwIfTypeMismatch("timestamp", jsonValue.isTextual(), jsonValue);
            if (jsonValue.isNull()) {
                return NULL_LONG_BLOCK;
            }
            else {
                Instant time = OffsetDateTime.parse(jsonValue.textValue()).toInstant();
                return new LongArrayBlock(1, NOT_NULL_BLOCK, new long[] {ChronoUnit.MICROS.between(Instant.EPOCH, time)});
            }
        }

        if (dataType instanceof TimestampNTZType) {
            throwIfTypeMismatch("timestamp_ntz", jsonValue.isTextual(), jsonValue);
            throw new UnsupportedOperationException("not yet supported");
        }

        if (dataType instanceof StructType structType) {
            throwIfTypeMismatch("object", jsonValue.isObject(), jsonValue);

            Block[] fieldBlocks = new Block[structType.length()];
            for (int i = 0; i < structType.length(); i++) {
                StructField field = ((StructType) dataType).at(i);
                Block parsedValue;
                if (jsonValue.isNull()) {
                    parsedValue = createBlockBuilder(typeManager, field.getDataType(), 0)
                            .appendNull()
                            .build();
                }
                else {
                    parsedValue = decodeField(typeManager, (ObjectNode) jsonValue, field);
                }
                fieldBlocks[i] = parsedValue;
            }
            return RowBlock.fromFieldBlocks(1, fieldBlocks);
        }

        if (dataType instanceof ArrayType) {
            throwIfTypeMismatch("array", jsonValue.isArray(), jsonValue);
            ArrayType arrayType = ((ArrayType) dataType);
            ArrayNode jsonArray = (ArrayNode) jsonValue;
            BlockBuilder blockBuilder = createBlockBuilder(typeManager, arrayType.getElementType(), jsonArray.size());
            for (int i = 0; i < jsonArray.size(); i++) {
                JsonNode element = jsonArray.get(i);
                ValueBlock blockElement = decodeElement(typeManager, element, arrayType.getElementType());
                blockBuilder.append(blockElement, i);
            }
            Block elements = blockBuilder.buildValueBlock();
            return ArrayBlock.fromElementBlock(jsonArray.size(), NOT_NULL_BLOCK, new int[] {0, jsonArray.size()}, elements);
        }

        if (dataType instanceof MapType) {
            throwIfTypeMismatch("map", jsonValue.isObject(), jsonValue);
            final MapType mapType = (MapType) dataType;
            if (!(mapType.getKeyType() instanceof StringType)) {
                throw new RuntimeException(
                        "MapType with a key type of `String` is supported, "
                                + "received a key type: "
                                + mapType.getKeyType());
            }
            BlockBuilder keysBuilder = createBlockBuilder(typeManager, mapType.getKeyType(), jsonValue.size());
            BlockBuilder valuesBuilder = createBlockBuilder(typeManager, mapType.getValueType(), jsonValue.size());

            final Iterator<Map.Entry<String, JsonNode>> iter = jsonValue.fields();

            int i = 0;
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();

                String keyParsed = entry.getKey();
                ValueBlock keyBlock = new VariableWidthBlock(
                        1,
                        Slices.utf8Slice(keyParsed),
                        new int[] {0, keyParsed.length()},
                        NOT_NULL_BLOCK);
                keysBuilder.append(keyBlock, i);

                ValueBlock valueBlock = decodeElement(typeManager, entry.getValue(), mapType.getValueType());
                valuesBuilder.append(valueBlock, i);

                i++;
            }

            return MapBlock.fromKeyValueBlock(
                    NOT_NULL_BLOCK,
                    new int[] {0, jsonValue.size()},
                    keysBuilder.buildValueBlock(),
                    valuesBuilder.buildValueBlock(),
                    (io.trino.spi.type.MapType) KernelSchemaUtils.toTrinoType(
                            new SchemaTableName("test", "test"),
                            typeManager,
                            mapType));
        }

        throw new UnsupportedOperationException(
                format("Unsupported DataType %s for RootNode %s", dataType, jsonValue));
    }

    private static Block decodeField(TypeManager typeManager, ObjectNode rootNode, StructField field)
    {
        if (rootNode.get(field.getName()) == null || rootNode.get(field.getName()).isNull()) {
            if (field.isNullable()) {
                return createBlockBuilder(typeManager, field.getDataType(), 0)
                        .appendNull()
                        .build();
            }

            throw new RuntimeException(
                    format(
                            "Root node at key %s is null but field isn't nullable. Root node: %s",
                            field.getName(), rootNode));
        }

        return decodeElement(typeManager, rootNode.get(field.getName()), field.getDataType());
    }

    private static void throwIfTypeMismatch(String expType, boolean hasExpType, JsonNode jsonNode)
    {
        if (!hasExpType) {
            throw new RuntimeException(
                    format("Couldn't decode %s, expected a %s", jsonNode, expType));
        }
    }
}
