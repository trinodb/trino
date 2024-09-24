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
package io.trino.plugin.pulsar.decoder.avro;

import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.avro.AvroColumnDecoder;
//import io.trino.decoder.DecoderColumnHandle;
//import io.trino.decoder.FieldValueProvider;
//import io.trino.decoder.avro.AvroColumnDecoder;
import io.trino.plugin.pulsar.PulsarConnectorUtils;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
//import io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_SCHEMA_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PulsarAvroRowDecoder
        implements PulsarRowDecoder
{
    private final GenericAvroSchema genericAvroSchema;
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;
    private final Schema stringSchema = Schema.create(Schema.Type.STRING);
    private final org.apache.avro.generic.GenericData genericData = new org.apache.avro.generic.GenericData();

    public PulsarAvroRowDecoder(GenericAvroSchema genericAvroSchema, Set<DecoderColumnHandle> columns)
    {
        this.genericAvroSchema = requireNonNull(genericAvroSchema, "genericAvroSchema is null");
        columnDecoders = columns.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    /**
     * Decode ByteBuf by {@link org.apache.pulsar.client.api.schema.GenericSchema}.
     *
     * @param byteBuf
     * @return
     */
    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf)
    {
        GenericRecord avroRecord;
        try {
            GenericAvroRecord record = (GenericAvroRecord) genericAvroSchema.decode(byteBuf);
            avroRecord = record.getAvroRecord();
        }
        catch (SchemaSerializationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Decoding avro record failed.", e);
        }
        return Optional.of(columnDecoders.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> {
            FieldValueProvider fieldValueProvider = entry.getValue().decodeField(toOriginalGenericRecord(avroRecord));
            if (entry.getKey().getType() == TimestampType.TIMESTAMP_MILLIS || entry.getKey().getType() == TIME_MILLIS) {
                fieldValueProvider = new PulsarAvroFieldValueProvider(fieldValueProvider, fieldValueProvider.getLong(), entry.getKey().getType(), entry.getKey().getName());
            }
            return fieldValueProvider;
        })));
    }

    private class PulsarAvroFieldValueProvider
            extends FieldValueProvider
    {
        private final FieldValueProvider delegate;
        private final Object value;
        private final Type columnType;
        private final String columnName;

        public PulsarAvroFieldValueProvider(FieldValueProvider delegate, Object value, Type columnType, String columnName)
        {
            this.delegate = delegate;
            this.value = value;
            this.columnType = columnType;
            this.columnName = columnName;
        }

        @Override
        public boolean isNull()
        {
            return delegate.isNull();
        }

        @Override
        public double getDouble()
        {
            return delegate.getDouble();
        }

        @Override
        public boolean getBoolean()
        {
            return delegate.getBoolean();
        }

        @Override
        public long getLong()
        {
            if (value instanceof Long) {
                return columnType == TimestampType.TIMESTAMP_MILLIS || columnType == TIME_MILLIS ?
                        PulsarConnectorUtils.roundToTrinoTime(((Number) value).longValue()) : ((Number) value).longValue();
            }

            if (columnType instanceof RealType) {
                return floatToIntBits((Float) value);
            }

            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public Slice getSlice()
        {
            return delegate.getSlice();
        }

        @Override
        public Block getBlock()
        {
            return delegate.getBlock();
        }
    }

    private Schema toOriginalSchema(Schema schema)
    {
        switch (schema.getType()) {
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
                return Schema.create(Schema.Type.valueOf(schema.getType().getName().toUpperCase(Locale.ENGLISH)));
            case RECORD:
                List<Schema.Field> fields = schema.getFields()
                        .stream()
                        .map(this::toOriginalField)
                        .collect(Collectors.toList());
                return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), fields);
            case ENUM:
                return Schema.createEnum(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.getEnumSymbols(), schema.getEnumDefault());
            case ARRAY:
                return Schema.createArray(toOriginalSchema(schema.getElementType()));
            case MAP:
                return Schema.createMap(toOriginalSchema(schema.getValueType()));
            case UNION:
                List<Schema> types = schema.getTypes()
                        .stream()
                        .map(this::toOriginalSchema)
                        .collect(Collectors.toList());
                return Schema.createUnion(types);
            case FIXED:
                return Schema.createFixed(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.getFixedSize());
            default:
                throw new TrinoException(PULSAR_SCHEMA_ERROR, format("schema type %s not supported", schema.getFullName()), null);
        }
    }

    private Schema.Field toOriginalField(Schema.Field field)
    {
        return new Schema.Field(
                field.name(),
                toOriginalSchema(field.schema()),
                field.doc(),
                null,
                Schema.Field.Order.valueOf(field.order().name().toUpperCase(Locale.ENGLISH)));
    }

    private Object toOriginalType(Object in, Schema schema)
    {
        if (in instanceof GenericRecord) {
            return toOriginalGenericRecord((GenericRecord) in);
        }
        else if (in instanceof Map) {
            return ((Map<?, ?>) in).entrySet().stream().map(entry -> {
                Schema valueSchema = null;
                // unnest union
                if (schema.isUnion()) {
                    for (Schema unionType : schema.getTypes()) {
                        if (unionType.getType() != Schema.Type.NULL) {
                            valueSchema = unionType.getValueType();
                        }
                    }
                }
                else {
                    valueSchema = schema.getValueType();
                }
                return Maps.immutableEntry(genericData.deepCopy(stringSchema, convertString(entry.getKey())), genericData.deepCopy(valueSchema, toOriginalType(entry.getValue(), valueSchema)));
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        else if (in instanceof GenericEnumSymbol) {
            return toOriginalGenericEnumSymbol((GenericEnumSymbol) in);
        }
        else if (in instanceof GenericFixed) {
            return toOriginalGenericFixed((GenericFixed) in);
        }
        else if (in instanceof GenericArray) {
            return toOriginalGenericArray((GenericArray) in, schema);
        }
        else if (in instanceof Utf8) {
            return toOriginalUtf8((Utf8) in);
        }
        return in;
    }

    private Object convertString(Object in)
    {
        if (in instanceof String) {
            return in;
        }
        else if (in instanceof Utf8) {
            return toOriginalUtf8((Utf8) in);
        }
        return new Utf8(in.toString());
    }

    private GenericRecord toOriginalGenericRecord(GenericRecord avroRecord)
    {
        Schema schema = toOriginalSchema(avroRecord.getSchema());
        org.apache.avro.generic.GenericRecordBuilder recordBuilder = new org.apache.avro.generic.GenericRecordBuilder(schema);
        for (int index = 0; index < avroRecord.getSchema().getFields().size(); index++) {
            recordBuilder.set(schema.getFields().get(index), toOriginalType(avroRecord.get(index), schema.getFields().get(index).schema()));
        }
        return recordBuilder.build();
    }

    private org.apache.avro.generic.GenericEnumSymbol toOriginalGenericEnumSymbol(GenericEnumSymbol enumSymbol)
    {
        return new org.apache.avro.generic.GenericData.EnumSymbol(toOriginalSchema(enumSymbol.getSchema()), enumSymbol.toString());
    }

    private org.apache.avro.generic.GenericArray toOriginalGenericArray(GenericArray array, Schema schema)
    {
        return new org.apache.avro.generic.GenericData.Array(toOriginalSchema(array.getSchema()), (Collection) array.subList(0, array.size()).stream().map(element -> toOriginalType(element, schema)).collect(Collectors.toCollection(ArrayList::new)));
    }

    private org.apache.avro.util.Utf8 toOriginalUtf8(Utf8 utf8)
    {
        return new org.apache.avro.util.Utf8(utf8.toString());
    }

    private org.apache.avro.generic.GenericFixed toOriginalGenericFixed(GenericFixed fixed)
    {
        return new org.apache.avro.generic.GenericData.Fixed(toOriginalSchema(fixed.getSchema()), fixed.bytes());
    }
}
