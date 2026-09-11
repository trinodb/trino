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
package io.trino.plugin.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapHashTables;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.math.RoundingMode.UNNECESSARY;

final class JsonTransportCodec
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private JsonTransportCodec() {}

    static Block readJsonArray(ResultSet rs, int columnIndex, ArrayType arrayType)
            throws SQLException
    {
        JsonNode jsonNode = readJsonNode(rs, columnIndex);
        if (jsonNode == null) {
            return null;
        }
        if (!jsonNode.isArray()) {
            throw new TrinoException(JDBC_ERROR, "Expected JSON array transport for type " + arrayType);
        }
        Type elementType = arrayType.getElementType();
        BlockBuilder builder = elementType.createBlockBuilder(null, jsonNode.size());
        for (JsonNode elementNode : jsonNode) {
            writeJsonNodeToBlock(elementNode, elementType, builder);
        }
        return builder.build();
    }

    static SqlMap readJsonMap(ResultSet rs, int columnIndex, MapType mapType)
            throws SQLException
    {
        JsonNode jsonNode = readJsonNode(rs, columnIndex);
        if (jsonNode == null) {
            return null;
        }
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        BlockBuilder keyBuilder = keyType.createBlockBuilder(null, jsonNode.size());
        BlockBuilder valueBuilder = valueType.createBlockBuilder(null, jsonNode.size());
        appendJsonMapEntries(jsonNode, mapType, keyBuilder, valueBuilder);
        return new SqlMap(mapType, MapHashTables.HashBuildMode.DUPLICATE_NOT_CHECKED, keyBuilder.build(), valueBuilder.build());
    }

    static SqlRow readJsonRow(ResultSet rs, int columnIndex, RowType rowType)
            throws SQLException
    {
        JsonNode jsonNode = readJsonNode(rs, columnIndex);
        if (jsonNode == null) {
            return null;
        }

        List<RowType.Field> fields = rowType.getFields();
        Block[] fieldBlocks = new Block[fields.size()];
        for (int index = 0; index < fields.size(); index++) {
            BlockBuilder fieldBuilder = fields.get(index).getType().createBlockBuilder(null, 1);
            JsonNode fieldNode = jsonNode.isArray() ? jsonNode.path(index) : getRowFieldNode(jsonNode, fields.get(index), index);
            writeJsonNodeToBlock(fieldNode, fields.get(index).getType(), fieldBuilder);
            fieldBlocks[index] = fieldBuilder.build();
        }
        return new SqlRow(0, fieldBlocks);
    }

    private static JsonNode readJsonNode(ResultSet rs, int columnIndex)
            throws SQLException
    {
        String json = rs.getString(columnIndex);
        if (json == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readTree(json);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(JDBC_ERROR, "Invalid JSON transport payload: " + json, e);
        }
    }

    private static JsonNode getRowFieldNode(JsonNode jsonNode, RowType.Field field, int index)
    {
        if (jsonNode.isValueNode()) {
            return index == 0 ? jsonNode : null;
        }
        if (!jsonNode.isObject()) {
            return jsonNode.path(index);
        }
        if (field.getName().isPresent()) {
            return jsonNode.path(field.getName().orElseThrow());
        }
        return jsonNode.path(index);
    }

    private static void writeJsonNodeToBlock(JsonNode node, Type type, BlockBuilder builder)
    {
        if (node == null || node.isMissingNode() || node.isNull()) {
            builder.appendNull();
            return;
        }
        if (node.isTextual() && (type instanceof ArrayType || type instanceof MapType || type instanceof RowType)) {
            writeStringValueToBlock(node.asText(), type, builder);
            return;
        }

        if (type instanceof ArrayType arrayType) {
            if (!node.isArray()) {
                throw new TrinoException(JDBC_ERROR, "Expected JSON array for type " + type);
            }
            Type elementType = arrayType.getElementType();
            BlockBuilder elementBuilder = elementType.createBlockBuilder(null, node.size());
            for (JsonNode elementNode : node) {
                writeJsonNodeToBlock(elementNode, elementType, elementBuilder);
            }
            type.writeObject(builder, elementBuilder.build());
            return;
        }

        if (type instanceof MapType mapType) {
            BlockBuilder keyBuilder = mapType.getKeyType().createBlockBuilder(null, node.size());
            BlockBuilder valueBuilder = mapType.getValueType().createBlockBuilder(null, node.size());
            appendJsonMapEntries(node, mapType, keyBuilder, valueBuilder);
            type.writeObject(builder, new SqlMap(mapType, MapHashTables.HashBuildMode.DUPLICATE_NOT_CHECKED, keyBuilder.build(), valueBuilder.build()));
            return;
        }

        if (type instanceof RowType rowType) {
            List<RowType.Field> fields = rowType.getFields();
            Block[] fieldBlocks = new Block[fields.size()];
            for (int index = 0; index < fields.size(); index++) {
                BlockBuilder fieldBuilder = fields.get(index).getType().createBlockBuilder(null, 1);
                writeJsonNodeToBlock(getRowFieldNode(node, fields.get(index), index), fields.get(index).getType(), fieldBuilder);
                fieldBlocks[index] = fieldBuilder.build();
            }
            type.writeObject(builder, new SqlRow(0, fieldBlocks));
            return;
        }

        writeStringValueToBlock(node.asText(), type, builder);
    }

    private static void writeStringValueToBlock(String value, Type type, BlockBuilder builder)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }

        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            try {
                writeJsonNodeToBlock(OBJECT_MAPPER.readTree(value), type, builder);
                return;
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(JDBC_ERROR, "Invalid JSON transport key/value: " + value, e);
            }
        }

        if (type instanceof CharType charType) {
            type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), charType));
            return;
        }
        if (type instanceof VarcharType) {
            type.writeSlice(builder, Slices.utf8Slice(value));
            return;
        }
        if (type == BIGINT) {
            type.writeLong(builder, Long.parseLong(value));
            return;
        }
        if (type == INTEGER) {
            type.writeLong(builder, Integer.parseInt(value));
            return;
        }
        if (type == SMALLINT) {
            type.writeLong(builder, Short.parseShort(value));
            return;
        }
        if (type == TINYINT) {
            type.writeLong(builder, Byte.parseByte(value));
            return;
        }
        if (type == DOUBLE) {
            type.writeDouble(builder, Double.parseDouble(value));
            return;
        }
        if (type == REAL) {
            type.writeLong(builder, Float.floatToIntBits(Float.parseFloat(value)));
            return;
        }
        if (type == BOOLEAN) {
            boolean booleanValue = switch (value) {
                case "true" -> true;
                case "false" -> false;
                default -> throw new TrinoException(JDBC_ERROR, "Invalid boolean JSON transport value: " + value);
            };
            type.writeBoolean(builder, booleanValue);
            return;
        }
        if (type instanceof VarbinaryType) {
            type.writeSlice(builder, Slices.wrappedBuffer(HEX_FORMAT.parseHex(value)));
            return;
        }
        if (type instanceof DateType) {
            type.writeLong(builder, TemporalTransportCodec.parseDate(value).toEpochDay());
            return;
        }
        if (type instanceof TimeType) {
            type.writeLong(builder, TemporalTransportCodec.parseTimeToPicos(value));
            return;
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                type.writeLong(builder, TemporalTransportCodec.parseShortTimestamp(value));
            }
            else {
                type.writeObject(builder, TemporalTransportCodec.parseLongTimestamp(value));
            }
            return;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.isShort()) {
                type.writeLong(builder, TimestampWithTimeZoneTransport.parseShortTimestampWithTimeZone(value));
            }
            else {
                type.writeObject(builder, TimestampWithTimeZoneTransport.parseLongTimestampWithTimeZone(value));
            }
            return;
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            if (timeWithTimeZoneType.isShort()) {
                type.writeLong(builder, TemporalTransportCodec.parseShortTimeWithTimeZone(value));
            }
            else {
                type.writeObject(builder, TemporalTransportCodec.parseLongTimeWithTimeZone(value));
            }
            return;
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = new BigDecimal(value);
            if (decimalType.isShort()) {
                type.writeLong(builder, decimal.setScale(decimalType.getScale(), UNNECESSARY).unscaledValue().longValueExact());
            }
            else {
                type.writeObject(builder, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
            }
            return;
        }
        if (type instanceof UuidType) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.uuidSlice(value));
            return;
        }
        if (TrinoTypeClassifier.isJsonType(type)) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.jsonSlice(value));
            return;
        }
        if (TrinoTypeClassifier.isIpAddressType(type)) {
            type.writeSlice(builder, TrinoSpecialTypeCodec.ipAddressSlice(value));
            return;
        }
        if (TrinoTypeClassifier.isNumberType(type)) {
            type.writeObject(builder, TrinoNumberCodec.parse(value));
            return;
        }
        if (TrinoTypeClassifier.isIntervalYearToMonthType(type) || TrinoTypeClassifier.isIntervalDayToSecondType(type)) {
            type.writeLong(builder, TemporalTransportCodec.parseIntervalValue(value, type));
            return;
        }
        throw new TrinoException(JDBC_ERROR, "Unsupported string transport type: " + type);
    }

    private static void appendJsonMapEntries(JsonNode node, MapType mapType, BlockBuilder keyBuilder, BlockBuilder valueBuilder)
    {
        if (JsonTransportHelper.usesJsonObjectKeyEncoding(mapType)) {
            if (!node.isObject()) {
                throw new TrinoException(JDBC_ERROR, "Expected JSON object transport for type " + mapType);
            }
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                writeStringValueToBlock(entry.getKey(), mapType.getKeyType(), keyBuilder);
                writeJsonNodeToBlock(entry.getValue(), mapType.getValueType(), valueBuilder);
            }
            return;
        }

        if (!node.isArray()) {
            throw new TrinoException(JDBC_ERROR, "Expected JSON entry array transport for type " + mapType);
        }
        for (JsonNode entryNode : node) {
            JsonNode keyNode = getMapEntryFieldNode(entryNode, "key", 0);
            JsonNode valueNode = getMapEntryFieldNode(entryNode, "value", 1);
            writeJsonNodeToBlock(keyNode, mapType.getKeyType(), keyBuilder);
            writeJsonNodeToBlock(valueNode, mapType.getValueType(), valueBuilder);
        }
    }

    private static JsonNode getMapEntryFieldNode(JsonNode entryNode, String fieldName, int index)
    {
        if (entryNode == null || entryNode.isNull()) {
            return null;
        }
        if (entryNode.isArray()) {
            return entryNode.path(index);
        }
        if (entryNode.isObject()) {
            JsonNode namedField = entryNode.get(fieldName);
            if (namedField != null) {
                return namedField;
            }
            return entryNode.get(String.valueOf(index));
        }
        throw new TrinoException(JDBC_ERROR, "Expected JSON row entry array or object but found: " + entryNode.getNodeType());
    }
}
