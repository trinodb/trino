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
package io.trino.plugin.iceberg;

import io.trino.spi.block.Block;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionData
        implements StructLike
{
    private static final String PARTITION_VALUES_FIELD = "partitionValues";
    private static final JsonMapper MAPPER = new JsonMapper(jsonFactory())
            .rebuild()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .build();

    private final Object[] partitionValues;

    public PartitionData(Object[] partitionValues)
    {
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
    }

    @Override
    public int size()
    {
        return partitionValues.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass)
    {
        Object value = partitionValues[pos];

        if (javaClass == ByteBuffer.class && value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
        }

        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        }

        throw new IllegalArgumentException(format("Wrong class [%s] for object class [%s]", javaClass.getName(), value.getClass().getName()));
    }

    @Override
    public <T> void set(int pos, T value)
    {
        partitionValues[pos] = value;
    }

    public static String toJson(StructLike structLike)
    {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = MAPPER.createGenerator(writer);
            generator.writeStartObject();
            generator.writeArrayPropertyStart(PARTITION_VALUES_FIELD);
            for (int i = 0; i < structLike.size(); i++) {
                generator.writePOJO(structLike.get(i, Object.class));
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (JacksonException e) {
            throw new IllegalArgumentException("JSON conversion failed for: " + structLike, e);
        }
    }

    public static PartitionData fromBlocks(List<Block> partitionValues, Type[] types, TypeManager typeManager)
    {
        if (types.length == 0) {
            return new PartitionData(new Object[0]);
        }

        Object[] values = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            io.trino.spi.type.Type trinoType = toTrinoType(types[i], typeManager);
            Object value = readNativeValue(trinoType, partitionValues.get(i), 0);
            if (value == null) {
                values[i] = null;
            }
            else {
                values[i] = convertTrinoValueToIceberg(trinoType, value);
            }
        }
        return new PartitionData(values);
    }

    public static PartitionData fromJson(String partitionDataAsJson, PartitionSpec partitionSpec)
    {
        Type[] types = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(partitionSpec.schema().findType(field.sourceId())))
                .toArray(Type[]::new);
        return fromJson(partitionDataAsJson, types);
    }

    public static PartitionData fromJson(String partitionDataAsJson, Type[] types)
    {
        if (partitionDataAsJson == null) {
            return null;
        }

        JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(partitionDataAsJson);
        }
        catch (JacksonException e) {
            throw new IllegalArgumentException("Conversion from JSON failed for PartitionData: " + partitionDataAsJson, e);
        }
        if (jsonNode.isNull()) {
            return null;
        }

        JsonNode partitionValues = jsonNode.get(PARTITION_VALUES_FIELD);
        Object[] objects = new Object[types.length];
        int index = 0;
        for (JsonNode partitionValue : partitionValues) {
            objects[index] = getValue(partitionValue, types[index]);
            index++;
        }
        return new PartitionData(objects);
    }

    public static Object getValue(JsonNode partitionValue, Type type)
    {
        if (partitionValue.isNull()) {
            return null;
        }
        return switch (type.typeId()) {
            case BOOLEAN -> partitionValue.asBoolean();
            case INTEGER, DATE -> partitionValue.asInt();
            case LONG, TIME, TIMESTAMP, TIMESTAMP_NANO -> partitionValue.asLong();
            case FLOAT -> {
                // NaN and infinities are written as strings
                if (partitionValue.isString()) {
                    yield parseSpecialFloat(partitionValue.stringValue());
                }
                yield partitionValue.floatValue();
            }
            case DOUBLE -> {
                if (partitionValue.isString()) {
                    yield parseSpecialDouble(partitionValue.stringValue());
                }
                yield partitionValue.doubleValue();
            }
            case STRING -> partitionValue.asString();
            case UUID -> UUID.fromString(partitionValue.asString());
            case FIXED, BINARY -> {
                try {
                    yield partitionValue.binaryValue();
                }
                catch (JacksonException e) {
                    throw new IllegalArgumentException("Failed during JSON conversion of " + partitionValue, e);
                }
            }
            case DECIMAL -> {
                Types.DecimalType decimalType = (Types.DecimalType) type;
                yield rescale(
                        partitionValue.decimalValue(),
                        createDecimalType(decimalType.precision(), decimalType.scale()));
            }
            // TODO https://github.com/trinodb/trino/issues/24538 Support variant type
            case VARIANT, GEOMETRY, GEOGRAPHY, UNKNOWN, LIST, MAP, STRUCT -> throw new UnsupportedOperationException("Type not supported as partition column: " + type);
        };
    }

    private static float parseSpecialFloat(String value)
    {
        return (float) parseSpecialDouble(value);
    }

    private static double parseSpecialDouble(String value)
    {
        if (value.equalsIgnoreCase("NaN")) {
            return Double.NaN;
        }
        return Double.parseDouble(value);
    }
}
