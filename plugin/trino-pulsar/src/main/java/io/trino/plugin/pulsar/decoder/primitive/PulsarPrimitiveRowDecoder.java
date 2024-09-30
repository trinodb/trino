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
package io.trino.plugin.pulsar.decoder.primitive;

import io.netty.buffer.ByteBuf;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.FieldValueProviders;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.spi.type.*;
import org.apache.pulsar.client.impl.schema.AbstractSchema;

import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.decoder.FieldValueProviders.*;
import static io.trino.plugin.pulsar.PulsarFieldValueProviders.doubleValueProvider;

public class PulsarPrimitiveRowDecoder
        implements PulsarRowDecoder {
    private final DecoderColumnHandle columnHandle;
    private final AbstractSchema schema;

    @SuppressWarnings("rawtypes")
    public PulsarPrimitiveRowDecoder(AbstractSchema schema, DecoderColumnHandle columnHandle) {
        this.columnHandle = columnHandle;
        this.schema = schema;
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
        if (columnHandle == null) {
            return Optional.empty();
        }

        Object value = schema.decode(byteBuf.array());
        Map<DecoderColumnHandle, FieldValueProvider> primitiveColumn = new HashMap<>();
        if (value == null) {
            primitiveColumn.put(columnHandle, FieldValueProviders.nullValueProvider());
        } else {
            Type type = columnHandle.getType();
            if (type instanceof BooleanType) {
                primitiveColumn.put(columnHandle, booleanValueProvider((Boolean) value));
            } else if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType
                    || type instanceof BigintType) {
                primitiveColumn.put(columnHandle, longValueProvider(Long.valueOf(value.toString())));
            } else if (type instanceof DoubleType) {
                primitiveColumn.put(columnHandle, doubleValueProvider(Double.valueOf(value.toString())));
            } else if (type instanceof RealType) {
                primitiveColumn.put(columnHandle, longValueProvider(
                        Float.floatToIntBits((Float.valueOf(value.toString())))));
            } else if (type instanceof VarbinaryType) {
                primitiveColumn.put(columnHandle, bytesValueProvider((byte[]) value));
            } else if (type instanceof VarcharType) {
                primitiveColumn.put(columnHandle, bytesValueProvider(value.toString().getBytes(StandardCharsets.UTF_8)));
            } else if (type instanceof DateType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Date) value).getTime()));
            } else if (type instanceof TimeType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Time) value).getTime()));
            } else if (type instanceof TimestampType) {
                primitiveColumn.put(columnHandle, longValueProvider(((Timestamp) value).getTime()));
            } else {
                primitiveColumn.put(columnHandle, bytesValueProvider(value.toString().getBytes(StandardCharsets.UTF_8)));
            }
        }
        return Optional.of(primitiveColumn);
    }
}
