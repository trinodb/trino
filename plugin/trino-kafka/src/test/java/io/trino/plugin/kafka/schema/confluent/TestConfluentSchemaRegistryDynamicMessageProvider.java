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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.decoder.protobuf.DummyDescriptorProvider;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.decoder.protobuf.ProtobufRowDecoder;
import io.trino.decoder.protobuf.ProtobufRowDecoderFactory;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.testing.TestingConnectorSession;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.frame;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.messageIndexes;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryDynamicMessageProvider.readerSchemaReference;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConfluentSchemaRegistryDynamicMessageProvider
{
    private static final String SCHEMA =
            """
            syntax = "proto3";
            message schema {
                string col_1 = 1;
                int64 col_2 = 2;
                map<string, string> attributes = 3;
                message Detail {
                    string note = 1;
                    message Deep {
                        int64 depth = 1;
                    }
                }
                Detail detail = 4;
            }
            enum Kind {
                PLAIN = 0;
            }
            message Other {
                string name = 1;
            }
            """;

    @Test
    public void testDecodeFirstMessage()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        Descriptor descriptor = ((ConfluentProtobufSchema) client.getSchemaById(schemaId)).getMessageDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), "Trino")
                .setField(descriptor.findFieldByName("col_2"), 42L)
                .build();

        DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(frame(schemaId, ImmutableList.of(0), message.toByteArray()));

        assertThat(decoded.getDescriptorForType()).isSameAs(descriptor);
        assertThat(decoded.getField(descriptor.findFieldByName("col_1"))).isEqualTo("Trino");
        assertThat(decoded.getField(descriptor.findFieldByName("col_2"))).isEqualTo(42L);
    }

    @Test
    public void testDecodeNestedAndLaterMessages()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentProtobufSchema schema = (ConfluentProtobufSchema) client.getSchemaById(schemaId);
        Descriptor detail = schema.getMessageDescriptor().findNestedTypeByName("Detail");
        Descriptor deep = detail.findNestedTypeByName("Deep");
        Descriptor other = schema.getFileDescriptor().findMessageTypeByName("Other");
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);

        DynamicMessage detailMessage = DynamicMessage.newBuilder(detail).setField(detail.findFieldByName("note"), "nested").build();
        DynamicMessage decodedDetail = provider.parseDynamicMessage(frame(schemaId, messageIndexes(detail), detailMessage.toByteArray()));
        assertThat(decodedDetail.getDescriptorForType()).isSameAs(detail);
        assertThat(decodedDetail.getField(detail.findFieldByName("note"))).isEqualTo("nested");

        DynamicMessage deepMessage = DynamicMessage.newBuilder(deep).setField(deep.findFieldByName("depth"), 3L).build();
        DynamicMessage decodedDeep = provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(0, 0, 0), deepMessage.toByteArray()));
        assertThat(decodedDeep.getDescriptorForType()).isSameAs(deep);
        assertThat(decodedDeep.getField(deep.findFieldByName("depth"))).isEqualTo(3L);

        DynamicMessage otherMessage = DynamicMessage.newBuilder(other).setField(other.findFieldByName("name"), "second").build();
        DynamicMessage decodedOther = provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(1), otherMessage.toByteArray()));
        assertThat(decodedOther.getDescriptorForType()).isSameAs(other);
        assertThat(decodedOther.getField(other.findFieldByName("name"))).isEqualTo("second");
    }

    @Test
    public void testDecodeRecordOfConfluentSerializer()
            throws Exception
    {
        // protoc places the entry message of a map field among the nested messages, where the field is
        // declared; the serializer registers it as an explicit message, which counts towards the indexes
        DescriptorProto row = DescriptorProto.newBuilder()
                .setName("Row")
                .addField(field("note", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL))
                .build();
        DescriptorProto message = DescriptorProto.newBuilder()
                .setName("schema")
                .addField(field("attributes", 1, Type.TYPE_MESSAGE, Label.LABEL_REPEATED).setTypeName(".test.schema.AttributesEntry"))
                .addField(field("row", 2, Type.TYPE_MESSAGE, Label.LABEL_OPTIONAL).setTypeName(".test.schema.Row"))
                .addNestedType(DescriptorProto.newBuilder()
                        .setName("AttributesEntry")
                        .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                        .addField(field("key", 1, Type.TYPE_STRING, Label.LABEL_OPTIONAL))
                        .addField(field("value", 2, Type.TYPE_STRING, Label.LABEL_OPTIONAL)))
                .addNestedType(row)
                .build();
        FileDescriptor protocFile = FileDescriptor.buildFrom(
                FileDescriptorProto.newBuilder()
                        .setName("test.proto")
                        .setSyntax("proto3")
                        .setPackage("test")
                        .addMessageType(message)
                        .build(),
                new FileDescriptor[0]);
        Descriptor protocRow = protocFile.findMessageTypeByName("schema").findNestedTypeByName("Row");
        Descriptor protocMessage = protocFile.findMessageTypeByName("schema");

        MockSchemaRegistryClient serializerClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        byte[] rowRecord;
        byte[] messageRecord;
        try (KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(serializerClient)) {
            serializer.configure(ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, "mock://registry"), false);
            rowRecord = serializer.serialize("rows", DynamicMessage.newBuilder(protocRow).setField(protocRow.findFieldByName("note"), "nested").build());
            messageRecord = serializer.serialize("messages", DynamicMessage.newBuilder(protocMessage)
                    .addRepeatedField(protocMessage.findFieldByName("attributes"), DynamicMessage.newBuilder(protocMessage.findNestedTypeByName("AttributesEntry"))
                            .setField(protocMessage.findNestedTypeByName("AttributesEntry").findFieldByName("key"), "size")
                            .setField(protocMessage.findNestedTypeByName("AttributesEntry").findFieldByName("value"), "large")
                            .build())
                    .build());
        }
        assertThat(ConfluentProtobufWireFormat.readFrame(rowRecord).messageIndexes()).containsExactly(0, 1);

        // register the text the serializer registered, under the same ids
        MockSchemaRegistryClient client = createClient();
        for (String subject : ImmutableList.of("rows-value", "messages-value")) {
            int id = serializerClient.getLatestSchemaMetadata(subject).getId();
            assertThat(client.register(subject, schema(client, serializerClient.getSchemaById(id).canonicalString()))).isEqualTo(id);
        }
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);

        DynamicMessage decodedRow = provider.parseDynamicMessage(rowRecord);
        assertThat(decodedRow.getDescriptorForType().getFullName()).isEqualTo("test.schema.Row");
        assertThat(decodedRow.getField(decodedRow.getDescriptorForType().findFieldByName("note"))).isEqualTo("nested");

        DynamicMessage decodedMessage = provider.parseDynamicMessage(messageRecord);
        assertThat(decodedMessage.getDescriptorForType().findFieldByName("attributes").isMapField()).isTrue();
        assertThat(decodedMessage.getRepeatedFieldCount(decodedMessage.getDescriptorForType().findFieldByName("attributes"))).isEqualTo(1);
    }

    @Test
    public void testDecodeProto3OptionalOfConfluentSerializer()
            throws Exception
    {
        // protoc represents a proto3 optional field as the only member of a synthetic oneof, which the
        // text the serializer registers must declare with the optional keyword for presence to survive
        DescriptorProto message = DescriptorProto.newBuilder()
                .setName("schema")
                .addField(field("count", 1, Type.TYPE_INT64, Label.LABEL_OPTIONAL).setProto3Optional(true).setOneofIndex(0))
                .addField(field("plain", 2, Type.TYPE_INT64, Label.LABEL_OPTIONAL))
                .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_count"))
                .build();
        FileDescriptor protocFile = FileDescriptor.buildFrom(
                FileDescriptorProto.newBuilder()
                        .setName("test.proto")
                        .setSyntax("proto3")
                        .addMessageType(message)
                        .build(),
                new FileDescriptor[0]);
        Descriptor protocMessage = protocFile.findMessageTypeByName("schema");
        FieldDescriptor protocCount = protocMessage.findFieldByName("count");

        MockSchemaRegistryClient serializerClient = new MockSchemaRegistryClient(ImmutableList.of(new ProtobufSchemaProvider()));
        byte[] unsetRecord;
        byte[] defaultRecord;
        try (KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(serializerClient)) {
            serializer.configure(ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, "mock://registry"), false);
            unsetRecord = serializer.serialize("counts", DynamicMessage.newBuilder(protocMessage).build());
            defaultRecord = serializer.serialize("counts", DynamicMessage.newBuilder(protocMessage).setField(protocCount, 0L).build());
        }

        MockSchemaRegistryClient client = createClient();
        int id = serializerClient.getLatestSchemaMetadata("counts-value").getId();
        assertThat(client.register("counts-value", schema(client, serializerClient.getSchemaById(id).canonicalString()))).isEqualTo(id);
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);

        DynamicMessage unset = provider.parseDynamicMessage(unsetRecord);
        FieldDescriptor count = unset.getDescriptorForType().findFieldByName("count");
        assertThat(count.hasPresence()).isTrue();
        assertThat(count.getRealContainingOneof()).isNull();
        assertThat(unset.hasField(count)).isFalse();
        assertThat(unset.getDescriptorForType().findFieldByName("plain").hasPresence()).isFalse();

        DynamicMessage setToDefault = provider.parseDynamicMessage(defaultRecord);
        assertThat(setToDefault.hasField(count)).isTrue();
        assertThat(setToDefault.getField(count)).isEqualTo(0L);
    }

    @Test
    public void testDecodeWithWriterSchema()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int oldId = client.register("topic-value", schema(client,
                """
                syntax = "proto3";
                message schema {
                    string col_1 = 1;
                }
                """));
        int newId = client.register("topic-value", schema(client, SCHEMA));
        assertThat(newId).isNotEqualTo(oldId);
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);

        Descriptor oldDescriptor = ((ConfluentProtobufSchema) client.getSchemaById(oldId)).getMessageDescriptor();
        byte[] oldRecord = frame(oldId, ImmutableList.of(0), DynamicMessage.newBuilder(oldDescriptor).setField(oldDescriptor.findFieldByName("col_1"), "old").build().toByteArray());
        Descriptor newDescriptor = ((ConfluentProtobufSchema) client.getSchemaById(newId)).getMessageDescriptor();
        byte[] newRecord = frame(newId, ImmutableList.of(0), DynamicMessage.newBuilder(newDescriptor).setField(newDescriptor.findFieldByName("col_1"), "new").setField(newDescriptor.findFieldByName("col_2"), 7L).build().toByteArray());

        assertThat(provider.parseDynamicMessage(oldRecord).getDescriptorForType()).isSameAs(oldDescriptor);
        assertThat(provider.parseDynamicMessage(newRecord).getDescriptorForType()).isSameAs(newDescriptor);
    }

    @Test
    public void testColumnsMissingFromWriterSchemaAreNull()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int oldId = client.register("topic-value", schema(client,
                """
                syntax = "proto3";
                message schema {
                    string col_1 = 1;
                    message Detail {
                        string note = 1;
                    }
                    Detail detail = 4;
                }
                """));
        int newId = client.register("topic-value", schema(client,
                """
                syntax = "proto3";
                message schema {
                    string col_1 = 1;
                    int64 col_2 = 2;
                    message Detail {
                        string note = 1;
                        int64 weight = 2;
                    }
                    Detail detail = 4;
                }
                """));

        // the table columns come from the latest schema
        RowType detailType = RowType.rowType(RowType.field("note", createUnboundedVarcharType()), RowType.field("weight", BIGINT));
        KafkaColumnHandle col1 = new KafkaColumnHandle("col_1", VARCHAR, "col_1", null, null, false, false, false, Optional.empty());
        KafkaColumnHandle col2 = new KafkaColumnHandle("col_2", BIGINT, "col_2", null, null, false, false, false, Optional.empty());
        KafkaColumnHandle detail = new KafkaColumnHandle("detail", detailType, "detail", null, null, false, false, false, Optional.empty());
        RowDecoder decoder = new ProtobufRowDecoderFactory(new ConfluentSchemaRegistryDynamicMessageProvider.Factory(client), TESTING_TYPE_MANAGER, new DummyDescriptorProvider())
                .create(TestingConnectorSession.SESSION, new RowDecoderSpec(ProtobufRowDecoder.NAME, ImmutableMap.of(), ImmutableSet.of(col1, col2, detail)));

        Descriptor oldDescriptor = ((ConfluentProtobufSchema) client.getSchemaById(oldId)).getMessageDescriptor();
        Descriptor oldDetail = oldDescriptor.findFieldByName("detail").getMessageType();
        DynamicMessage oldMessage = DynamicMessage.newBuilder(oldDescriptor)
                .setField(oldDescriptor.findFieldByName("col_1"), "old")
                .setField(oldDescriptor.findFieldByName("detail"), DynamicMessage.newBuilder(oldDetail).setField(oldDetail.findFieldByName("note"), "written before weight existed").build())
                .build();
        Map<DecoderColumnHandle, FieldValueProvider> oldRow = decoder.decodeRow(frame(oldId, ImmutableList.of(0), oldMessage.toByteArray())).orElseThrow();
        assertThat(oldRow.get(col1).getSlice().toStringUtf8()).isEqualTo("old");
        assertThat(oldRow.get(col2).isNull()).isTrue();
        SqlRow oldDetailRow = (SqlRow) oldRow.get(detail).getObject();
        assertThat(oldDetailRow.getRawFieldBlock(0).isNull(oldDetailRow.getRawIndex())).isFalse();
        assertThat(createUnboundedVarcharType().getSlice(oldDetailRow.getRawFieldBlock(0), oldDetailRow.getRawIndex()).toStringUtf8()).isEqualTo("written before weight existed");
        assertThat(oldDetailRow.getRawFieldBlock(1).isNull(oldDetailRow.getRawIndex())).isTrue();

        Descriptor newDescriptor = ((ConfluentProtobufSchema) client.getSchemaById(newId)).getMessageDescriptor();
        Descriptor newDetail = newDescriptor.findFieldByName("detail").getMessageType();
        DynamicMessage newMessage = DynamicMessage.newBuilder(newDescriptor)
                .setField(newDescriptor.findFieldByName("col_1"), "new")
                .setField(newDescriptor.findFieldByName("col_2"), 2L)
                .setField(newDescriptor.findFieldByName("detail"), DynamicMessage.newBuilder(newDetail).setField(newDetail.findFieldByName("note"), "n").setField(newDetail.findFieldByName("weight"), 9L).build())
                .build();
        Map<DecoderColumnHandle, FieldValueProvider> newRow = decoder.decodeRow(frame(newId, ImmutableList.of(0), newMessage.toByteArray())).orElseThrow();
        assertThat(newRow.get(col1).getSlice().toStringUtf8()).isEqualTo("new");
        assertThat(newRow.get(col2).getLong()).isEqualTo(2L);
        SqlRow newDetailRow = (SqlRow) newRow.get(detail).getObject();
        assertThat(BIGINT.getLong(newDetailRow.getRawFieldBlock(1), newDetailRow.getRawIndex())).isEqualTo(9L);
    }

    @Test
    public void testRenamedFieldsAreReadByNumber()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int oldId = client.register("topic-value", schema(client,
                """
                syntax = "proto3";
                message schema {
                    string col_1 = 1;
                    message Detail {
                        string note = 1;
                    }
                    Detail detail = 2;
                }
                """));
        int newId = client.register("topic-value", schema(client,
                """
                syntax = "proto3";
                message schema {
                    string name = 1;
                    message Detail {
                        string text = 1;
                    }
                    Detail info = 2;
                }
                """));

        // the table columns and the data schema come from the latest schema, which renamed every field
        RowType infoType = RowType.rowType(RowType.field("text", createUnboundedVarcharType()));
        KafkaColumnHandle name = new KafkaColumnHandle("name", VARCHAR, "name", null, null, false, false, false, Optional.empty());
        KafkaColumnHandle info = new KafkaColumnHandle("info", infoType, "info", null, null, false, false, false, Optional.empty());
        KafkaColumnHandle text = new KafkaColumnHandle("text", VARCHAR, "info/text", null, null, false, false, false, Optional.empty());
        RowDecoder decoder = new ProtobufRowDecoderFactory(new ConfluentSchemaRegistryDynamicMessageProvider.Factory(client), TESTING_TYPE_MANAGER, new DummyDescriptorProvider())
                .create(TestingConnectorSession.SESSION, new RowDecoderSpec(ProtobufRowDecoder.NAME, ImmutableMap.of("dataSchema", readerSchemaReference(newId, ((ConfluentProtobufSchema) client.getSchemaById(newId)).getMessageDescriptor())), ImmutableSet.of(name, info, text)));

        Descriptor oldDescriptor = ((ConfluentProtobufSchema) client.getSchemaById(oldId)).getMessageDescriptor();
        Descriptor oldDetail = oldDescriptor.findFieldByName("detail").getMessageType();
        DynamicMessage oldMessage = DynamicMessage.newBuilder(oldDescriptor)
                .setField(oldDescriptor.findFieldByName("col_1"), "old")
                .setField(oldDescriptor.findFieldByName("detail"), DynamicMessage.newBuilder(oldDetail).setField(oldDetail.findFieldByName("note"), "written before the rename").build())
                .build();
        Map<DecoderColumnHandle, FieldValueProvider> oldRow = decoder.decodeRow(frame(oldId, ImmutableList.of(0), oldMessage.toByteArray())).orElseThrow();
        assertThat(oldRow.get(name).getSlice().toStringUtf8()).isEqualTo("old");
        assertThat(oldRow.get(text).getSlice().toStringUtf8()).isEqualTo("written before the rename");
        SqlRow oldInfo = (SqlRow) oldRow.get(info).getObject();
        assertThat(createUnboundedVarcharType().getSlice(oldInfo.getRawFieldBlock(0), oldInfo.getRawIndex()).toStringUtf8()).isEqualTo("written before the rename");
    }

    @Test
    public void testReaderSchema()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentProtobufSchema schema = (ConfluentProtobufSchema) client.getSchemaById(schemaId);
        Descriptor first = schema.getMessageDescriptor();
        Descriptor deep = first.findNestedTypeByName("Detail").findNestedTypeByName("Deep");
        ConfluentSchemaRegistryDynamicMessageProvider.Factory factory = new ConfluentSchemaRegistryDynamicMessageProvider.Factory(client);

        assertThat(factory.create(Optional.empty()).getReaderDescriptor()).isEmpty();
        assertThat(readerSchemaReference(schemaId, first)).isEqualTo("schema-registry-id:" + schemaId + ";message=schema");
        assertThat(factory.create(Optional.of(readerSchemaReference(schemaId, first))).getReaderDescriptor()).contains(first);
        // the reader can be a nested message, and is the same descriptor instance records decode with
        assertThat(readerSchemaReference(schemaId, deep)).isEqualTo("schema-registry-id:" + schemaId + ";message=schema.Detail.Deep");
        DynamicMessageProvider deepProvider = factory.create(Optional.of(readerSchemaReference(schemaId, deep)));
        assertThat(deepProvider.getReaderDescriptor()).contains(deep);
        assertThat(deepProvider.parseDynamicMessage(frame(schemaId, messageIndexes(deep), new byte[0])).getDescriptorForType()).isSameAs(deepProvider.getReaderDescriptor().orElseThrow());
        assertTrinoException(
                () -> factory.create(Optional.of(readerSchemaReference(12345, first))).getReaderDescriptor(),
                KAFKA_SCHEMA_ERROR,
                "Schema id 12345 does not exist in the schema registry");
        assertTrinoException(
                () -> factory.create(Optional.of("schema-registry-id:" + schemaId + ";message=schema.Missing")).getReaderDescriptor(),
                KAFKA_SCHEMA_ERROR,
                "Schema id " + schemaId + " does not declare message schema.Missing");
        for (String reference : ImmutableList.of(SCHEMA, "schema-registry-id:" + schemaId, "schema-registry-id:" + schemaId + ";message=", "schema-registry-id:x;message=schema", "schema-registry-id:99999999999;message=schema")) {
            assertThatThrownBy(() -> factory.create(Optional.of(reference)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("Data schema is not a schema registry reference: " + reference.substring(0, 6));
        }
    }

    @Test
    public void testSchemaWithReferences()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client,
                """
                syntax = "proto3";
                package base;
                message Address {
                    string city = 1;
                }
                """));
        int schemaId = client.register("order-value", schema(
                client,
                """
                syntax = "proto3";
                import "address.proto";
                message Order {
                    base.Address address = 1;
                }
                """,
                new SchemaReference("address.proto", "address-value", 1)));
        Descriptor order = ((ConfluentProtobufSchema) client.getSchemaById(schemaId)).getMessageDescriptor();
        Descriptor address = order.findFieldByName("address").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("address"), DynamicMessage.newBuilder(address).setField(address.findFieldByName("city"), "Boston").build())
                .build();

        DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(frame(schemaId, ImmutableList.of(0), message.toByteArray()));
        DynamicMessage decodedAddress = (DynamicMessage) decoded.getField(order.findFieldByName("address"));
        assertThat(decodedAddress.getField(address.findFieldByName("city"))).isEqualTo("Boston");
    }

    @Test
    public void testUnknownSchemaId()
    {
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(createClient());
        assertTrinoException(() -> provider.parseDynamicMessage(frame(99, ImmutableList.of(0), new byte[0])), KAFKA_SCHEMA_ERROR, "Schema id 99 does not exist in the schema registry");
    }

    @Test
    public void testUnknownMessageIndexes()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        assertTrinoException(() -> provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(2), new byte[0])), INVALID_PROTOBUF_MESSAGE, "Message indexes [2] do not exist in schema id " + schemaId);
        assertTrinoException(() -> provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(0, 1), new byte[0])), INVALID_PROTOBUF_MESSAGE, "Message indexes [0, 1] do not exist in schema id " + schemaId);
    }

    @Test
    public void testNonProtobufSchemaId()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", new AvroSchema(SchemaBuilder.record("schema").fields().name("col_1").type().stringType().noDefault().endRecord()));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        assertTrinoException(() -> provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(0), new byte[0])), KAFKA_SCHEMA_ERROR, "Schema id " + schemaId + " is a AVRO schema, not a PROTOBUF schema");
    }

    @Test
    public void testMalformedRecord()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        assertTrinoException(() -> provider.parseDynamicMessage(new byte[] {1, 2, 3}), INVALID_PROTOBUF_MESSAGE, "Record is too short to be in the Confluent wire format");
        // field 1 is declared as a string but written as a truncated length-delimited value
        assertTrinoException(() -> provider.parseDynamicMessage(frame(schemaId, ImmutableList.of(0), new byte[] {0x0A, 0x05, 0x01})), INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.");
    }

    @Test
    public void testRegistryFailures()
            throws Exception
    {
        AtomicBoolean fail = new AtomicBoolean(true);
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(ImmutableList.of(new ConfluentProtobufSchemaProvider()))
        {
            @Override
            public ParsedSchema getSchemaById(int id)
                    throws IOException, RestClientException
            {
                if (fail.get()) {
                    throw new IOException("Connection refused");
                }
                return super.getSchemaById(id);
            }
        };
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        byte[] record = frame(schemaId, ImmutableList.of(0), new byte[0]);

        assertTrinoException(() -> provider.parseDynamicMessage(record), KAFKA_SCHEMA_REGISTRY_ERROR, "Failed to look up schema id " + schemaId + " in the schema registry: Connection refused");

        // the failure is not cached
        fail.set(false);
        assertThat(provider.parseDynamicMessage(record).getDescriptorForType().getName()).isEqualTo("schema");
    }

    @Test
    public void testRegistryErrorStatus()
            throws Exception
    {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(ImmutableList.of(new ConfluentProtobufSchemaProvider()))
        {
            @Override
            public ParsedSchema getSchemaById(int id)
                    throws RestClientException
            {
                throw new RestClientException("Internal Server Error", 500, 50001);
            }
        };
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        assertTrinoException(() -> provider.parseDynamicMessage(frame(1, ImmutableList.of(0), new byte[0])), KAFKA_SCHEMA_REGISTRY_ERROR, "Failed to look up schema id 1 in the schema registry: Internal Server Error; error code: 50001");
    }

    @Test
    public void testDescriptorsAreCached()
            throws Exception
    {
        AtomicInteger lookups = new AtomicInteger();
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(ImmutableList.of(new ConfluentProtobufSchemaProvider()))
        {
            @Override
            public ParsedSchema getSchemaById(int id)
                    throws IOException, RestClientException
            {
                lookups.incrementAndGet();
                return super.getSchemaById(id);
            }
        };
        int schemaId = client.register("topic-value", schema(client, SCHEMA));
        ConfluentSchemaRegistryDynamicMessageProvider provider = new ConfluentSchemaRegistryDynamicMessageProvider(client);
        byte[] record = frame(schemaId, ImmutableList.of(0), new byte[0]);
        byte[] nestedRecord = frame(schemaId, ImmutableList.of(0, 0), new byte[0]);

        provider.parseDynamicMessage(record);
        provider.parseDynamicMessage(record);
        assertThat(lookups.get()).isEqualTo(1);
        provider.parseDynamicMessage(nestedRecord);
        provider.parseDynamicMessage(nestedRecord);
        assertThat(lookups.get()).isEqualTo(2);
    }

    private static FieldDescriptorProto.Builder field(String name, int number, Type type, Label label)
    {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(label);
    }

    private static MockSchemaRegistryClient createClient()
    {
        return new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
    }

    private static ConfluentProtobufSchema schema(MockSchemaRegistryClient client, String schema, SchemaReference... references)
    {
        return new ConfluentProtobufSchema(schema, List.of(references), client);
    }

    private static void assertTrinoException(Runnable runnable, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()))
                .hasMessage(message);
    }
}
