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
package io.prestosql.plugin.kafka.encoder.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.AbstractRowEncoder;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonRowEncoder
        extends AbstractRowEncoder
{
    private static final Set<Type> PRIMITIVE_SUPPORTED_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, BOOLEAN);

    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private final ObjectNode node;

    JsonRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, ObjectMapper objectMapper)
    {
        super(session, columnHandles);

        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "Unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());
        }

        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.node = objectMapper.createObjectNode();
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) ||
                PRIMITIVE_SUPPORTED_TYPES.contains(type);
    }

    private String currentColumnName()
    {
        return columnHandles.get(currentColumnIndex).getName();
    }

    @Override
    protected void appendNullValue()
    {
        node.putNull(currentColumnName());
    }

    @Override
    protected void appendLong(long value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendInt(int value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendShort(short value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendByte(byte value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendDouble(double value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendFloat(float value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendString(String value)
    {
        node.put(currentColumnName(), value);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        node.put(currentColumnName(), value.array());
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        try {
            resetColumnIndex(); // reset currentColumnIndex to prepare for next row
            return objectMapper.writeValueAsBytes(node);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
