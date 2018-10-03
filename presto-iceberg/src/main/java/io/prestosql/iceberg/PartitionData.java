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
package io.prestosql.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.types.Type;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PartitionData
        implements StructLike
{
    private final Object[] partitionValues;

    private static final String PARTITON_VALUED_FIELD = "partitionValues";
    private static final JsonFactory FACTORY = new JsonFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

    public PartitionData(Object[] partitionValues)
    {
        this.partitionValues = partitionValues;
    }

    @Override
    public int size()
    {
        return partitionValues.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass)
    {
        Object v = partitionValues[pos];

        // Handling VarBinary and Fixed types as special case (Hate this so gotta find alternatives.)
        if (javaClass == ByteBuffer.class && v instanceof byte[]) {
            v = ByteBuffer.wrap((byte[]) v);
        }

        if (v == null || javaClass.isInstance(v)) {
            return javaClass.cast(v);
        }

        throw new IllegalArgumentException(String.format(
                "Wrong class, %s, for object: %s",
                javaClass.getName(), String.valueOf(v)));
    }

    @Override
    public <T> void set(int pos, T value)
    {
        partitionValues[pos] = value;
    }

    public String toJson()
    {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = FACTORY.createGenerator(writer);
            generator.writeStartObject();
            generator.writeArrayFieldStart(PARTITON_VALUED_FIELD);
            for (int i = 0; i < this.partitionValues.length; i++) {
                final Object value = this.partitionValues[i];
                if (value == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeObject(value);
                }
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e, "Json conversion failed for PartitionData = ", Arrays.toString(partitionValues));
        }
    }

    public static PartitionData fromJson(String partitionDataAsJson, Type[] types)
    {
        if (partitionDataAsJson == null) {
            return null;
        }

        final JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(partitionDataAsJson);
        }
        catch (IOException e) {
            throw new RuntimeIOException("conversion from Json failed for PartitionData = " + partitionDataAsJson, e);
        }
        if (jsonNode.isNull()) {
            return null;
        }

        final JsonNode partitionValues = jsonNode.get(PARTITON_VALUED_FIELD);
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
        switch (type.typeId()) {
            case BOOLEAN:
                return partitionValue.asBoolean();
            case INTEGER:
                return partitionValue.asInt();
            case LONG:
                return partitionValue.asLong();
            case FLOAT:
                return partitionValue.floatValue();
            case DOUBLE:
                return partitionValue.doubleValue();
            case DATE:
                return partitionValue.asInt();
            case TIMESTAMP:
                return partitionValue.asLong();
            case STRING:
                return partitionValue.asText();
            case UUID:
                return partitionValue.asText();
            case FIXED:
            case BINARY:
                try {
                    return partitionValue.binaryValue();
                }
                catch (IOException e) {
                    throw new RuntimeIOException("Failed during json conversion of " + partitionValue, e);
                }
            case DECIMAL:
                return partitionValue.decimalValue();
            default:
                throw new UnsupportedOperationException(type + " is not supported as partition column");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionData that = (PartitionData) o;

        Object[] a = this.partitionValues;
        Object[] a2 = that.partitionValues;

        if (a == a2) {
            return true;
        }
        if (a == null || a2 == null) {
            return false;
        }

        int length = a.length;
        if (a2.length != length) {
            return false;
        }

        for (int i = 0; i < length; i++) {
            Object o1 = a[i];
            Object o2 = a2[i];
            if (!(o1 == null ? o2 == null : o1.equals(o2))) {
                if (o1 != null && o1 instanceof byte[] && Arrays.equals((byte[]) o1, (byte[]) o2)) {
                    continue;
                }
                else {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        if (partitionValues == null) {
            return 0;
        }

        int result = 1;

        for (Object element : partitionValues) {
            result = 31 * result + (element == null ? 0 : element instanceof byte[] ? Arrays.hashCode((byte[]) element) : element.hashCode());
        }

        return result;
    }
}
