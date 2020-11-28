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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.avro.AvroBytesDeserializer;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.plugin.kafka.KafkaColumnHandle;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.decoder.avro.AvroRowDecoderFactory.DATA_SCHEMA;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestAvroConfluentRowDecoder
{
    private static final String TOPIC = "test";

    @Test
    public void testDecodingRows()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Schema initialSchema = SchemaBuilder.record(TOPIC)
                .fields()
                .name("col1").type().intType().noDefault()
                .name("col2").type().stringType().noDefault()
                .endRecord();

        Schema evolvedSchema = SchemaBuilder.record(TOPIC)
                .fields()
                .name("col1").type().intType().noDefault()
                .name("col2").type().stringType().noDefault()
                .name("col3").type().optional().longType()
                .endRecord();

        mockSchemaRegistryClient.register(format("%s-value", TOPIC), initialSchema);
        mockSchemaRegistryClient.register(format("%s-value", TOPIC), evolvedSchema);

        Set<DecoderColumnHandle> columnHandles = ImmutableSet.<DecoderColumnHandle>builder()
                .add(new KafkaColumnHandle("col1", INTEGER, "col1", null, null, false, false, false))
                .add(new KafkaColumnHandle("col2", VARCHAR, "col2", null, null, false, false, false))
                .add(new KafkaColumnHandle("col1", BIGINT, "col1", null, null, false, false, false))
                .build();

        RowDecoder rowDecoder = getRowDecoder(mockSchemaRegistryClient, columnHandles, evolvedSchema);
        testRow(rowDecoder, generateRecord(initialSchema, Arrays.asList(3, format("string-%s", 3))), 1);
        testRow(rowDecoder, generateRecord(evolvedSchema, Arrays.asList(4, format("string-%s", 4), 4L)), 2);
        testRow(rowDecoder, generateRecord(evolvedSchema, Arrays.asList(5, format("string-%s", 5), null)), 2);
    }

    @Test
    public void testSingleValueRow()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Schema schema = Schema.create(Schema.Type.LONG);
        mockSchemaRegistryClient.register(format("%s-key", TOPIC), schema);
        Set<DecoderColumnHandle> columnHandles = ImmutableSet.<DecoderColumnHandle>builder()
                .add(new KafkaColumnHandle("col1", BIGINT, "col1", null, null, false, false, false))
                .build();
        RowDecoder rowDecoder = getRowDecoder(mockSchemaRegistryClient, columnHandles, schema);
        testSingleValueRow(rowDecoder, 3L, schema, 1);
    }

    private void testRow(RowDecoder rowDecoder, GenericRecord record, int schemaId)
    {
        byte[] serializedRecord = serializeRecord(record, record.getSchema(), schemaId);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(serializedRecord, null);
        assertRowsAreEqual(decodedRow, record);
    }

    private void testSingleValueRow(RowDecoder rowDecoder, Object value, Schema schema, int schemaId)
    {
        byte[] serializedRecord = serializeRecord(value, schema, schemaId);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(serializedRecord, null);
        checkState(decodedRow.isPresent(), "decodedRow is not present");
        Map.Entry<DecoderColumnHandle, FieldValueProvider> entry = getOnlyElement(decodedRow.get().entrySet());
        assertValuesAreEqual(entry.getValue(), value, schema.getType());
    }

    private byte[] serializeRecord(Object record, Schema schema, int schemaId)
    {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(0);
            outputStream.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
            GenericDatumWriter<Object> avroRecordWriter = new GenericDatumWriter<>(schema);
            avroRecordWriter.write(record, encoder);
            encoder.flush();
            byte[] serializedRecord = outputStream.toByteArray();
            outputStream.close();
            return serializedRecord;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RowDecoder getRowDecoder(SchemaRegistryClient schemaRegistryClient, Set<DecoderColumnHandle> columnHandles, Schema schema)
    {
        ImmutableMap<String, String> decoderParams = ImmutableMap.<String, String>builder()
                .put(DATA_SCHEMA, schema.toString())
                .build();
        return getAvroRowDecoderyFactory(schemaRegistryClient).create(decoderParams, columnHandles);
    }

    public static final AvroRowDecoderFactory getAvroRowDecoderyFactory(SchemaRegistryClient schemaRegistryClient)
    {
        return new AvroRowDecoderFactory(new ConfluentAvroReaderSupplier.Factory(schemaRegistryClient), new AvroBytesDeserializer.Factory());
    }

    private void assertRowsAreEqual(Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow, GenericRecord expected)
    {
        checkState(decodedRow.isPresent(), "decoded row is not present");
        for (Map.Entry<DecoderColumnHandle, FieldValueProvider> entry : decodedRow.get().entrySet()) {
            String columnName = entry.getKey().getName();
            assertValuesAreEqual(entry.getValue(), expected.get(columnName), expected.getSchema().getField(columnName).schema().getType());
        }
    }

    private void assertValuesAreEqual(FieldValueProvider actual, Object expected, Schema.Type avroType)
    {
        if (actual.isNull()) {
            assertNull(expected);
        }
        else {
            switch (avroType) {
                case INT:
                case LONG:
                    assertEquals(actual.getLong(), ((Number) expected).longValue());
                    break;
                case STRING:
                    assertEquals(actual.getSlice().toStringUtf8(), expected);
                    break;
                default:
                    throw new IllegalStateException("Unexpected type");
            }
        }
    }

    private GenericRecord generateRecord(Schema schema, List<Object> values)
    {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for (int i = 0; i < values.size(); i++) {
            recordBuilder.set(schema.getFields().get(i), values.get(i));
        }
        return recordBuilder.build();
    }
}
