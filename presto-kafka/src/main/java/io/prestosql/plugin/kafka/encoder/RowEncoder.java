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
package io.prestosql.plugin.kafka.encoder;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;

import java.nio.ByteBuffer;

import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;

public interface RowEncoder
{
    byte[] toByteArray();

    void clear();

    RowEncoder putNullValue(EncoderColumnHandle columnHandle);

    default RowEncoder put(EncoderColumnHandle columnHandle, Object value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, long value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", long.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, int value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", int.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, short value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", short.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, byte value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", byte.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, double value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", double.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, float value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", float.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, boolean value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", boolean.class.getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, String value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, ByteBuffer value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, byte[] value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, SqlDate value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, SqlTime value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, SqlTimeWithTimeZone value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, SqlTimestamp value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }

    default RowEncoder put(EncoderColumnHandle columnHandle, SqlTimestampWithTimeZone value)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandle.getName()));
    }
}
