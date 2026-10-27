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
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

public class TestConfluentProtobufSchema
{
    private static final String ADDRESS_PROTO =
            """
            syntax = "proto3";
            package base;
            message Address {
                string city = 1;
            }
            """;

    private static final String ORDER_PROTO =
            """
            syntax = "proto3";
            import "address.proto";
            message Order {
                int64 id = 1;
                base.Address shipping_address = 2;
            }
            """;

    @Test
    public void testParseThroughClient()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        ParsedSchema parsed = client.parseSchema(ConfluentProtobufSchema.TYPE, ADDRESS_PROTO, ImmutableList.of()).orElseThrow();
        assertThat(parsed).isInstanceOf(ConfluentProtobufSchema.class);
        assertThat(parsed.schemaType()).isEqualTo("PROTOBUF");
        assertThat(parsed.canonicalString()).isEqualTo(ADDRESS_PROTO);
        assertThat(parsed.rawSchema()).isEqualTo(ADDRESS_PROTO);
        assertThat(parsed.name()).isEqualTo("base.Address");
        assertThat(parsed.references()).isEmpty();
        assertThat(parsed.version()).isNull();

        int id = client.register("address-value", parsed);
        ParsedSchema fetched = client.getSchemaById(id);
        assertThat(fetched).isEqualTo(parsed);
        assertThat(((ConfluentProtobufSchema) fetched).getMessageDescriptor().getFullName()).isEqualTo("base.Address");
    }

    @Test
    public void testParseThroughProvider()
    {
        ConfluentProtobufSchemaProvider provider = new ConfluentProtobufSchemaProvider();
        Schema entity = new Schema("subject", 3, 17, "PROTOBUF", null, ADDRESS_PROTO);
        ConfluentProtobufSchema schema = (ConfluentProtobufSchema) provider.parseSchemaOrElseThrow(entity, false, false);
        assertThat(schema.version()).isEqualTo(3);
        assertThat(schema.references()).isEmpty();
        assertThat(schema.getMessageDescriptor().getName()).isEqualTo("Address");
        assertThat(provider.parseSchema(entity, false, false)).isPresent();
    }

    @Test
    public void testFirstMessageIsTheTableMessage()
    {
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                enum Kind {
                    PLAIN = 0;
                }
                message Second {
                    string name = 1;
                }
                message First {
                    Kind kind = 1;
                }
                """);
        assertThat(schema.getMessageDescriptor().getName()).isEqualTo("Second");
        assertThat(schema.name()).isEqualTo("Second");
        assertThat(schema.getFileDescriptor().getMessageTypes()).hasSize(2);
        assertThat(schema.getFileDescriptor().getEnumTypes()).hasSize(1);
    }

    @Test
    public void testSubjectSelectsTheTableMessage()
    {
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                package shop;
                message Header {
                    string name = 1;
                }
                enum Status {
                    UNKNOWN = 0;
                }
                message Order {
                    int64 id = 1;
                    map<string, string> attributes = 2;
                    message Line {
                        string sku = 1;
                        message Discount {
                            string code = 1;
                        }
                    }
                }
                """);
        // a subject of the TopicNameStrategy does not name a message
        assertThat(schema.getMessageDescriptor("orders-value").getFullName()).isEqualTo("shop.Header");
        assertThat(schema.getMessageDescriptor("orders-key").getFullName()).isEqualTo("shop.Header");
        // the RecordNameStrategy names the subject after the message
        assertThat(schema.getMessageDescriptor("shop.Header").getFullName()).isEqualTo("shop.Header");
        assertThat(schema.getMessageDescriptor("shop.Order").getFullName()).isEqualTo("shop.Order");
        assertThat(schema.getMessageDescriptor("shop.Order.Line").getFullName()).isEqualTo("shop.Order.Line");
        assertThat(schema.getMessageDescriptor("shop.Order.Line.Discount").getFullName()).isEqualTo("shop.Order.Line.Discount");
        // the TopicRecordNameStrategy names it after the topic and the message
        assertThat(schema.getMessageDescriptor("orders-shop.Order").getFullName()).isEqualTo("shop.Order");
        assertThat(schema.getMessageDescriptor("orders-shop.Order.Line").getFullName()).isEqualTo("shop.Order.Line");
        assertThat(schema.getMessageDescriptor("my-orders-shop.Order.Line.Discount").getFullName()).isEqualTo("shop.Order.Line.Discount");
        // only the full name of a message identifies it
        for (String subject : ImmutableList.of("shop.Missing", "orders-shop.Missing", "Order", "orders-Order", "shop.Order-value", "orders-xshop.Order", "orders-shop.Order.AttributesEntry")) {
            assertThat(schema.getMessageDescriptor(subject).getFullName()).as(subject).isEqualTo("shop.Header");
        }

        assertThat(schema.findMessage("shop.Order.Line")).contains(schema.getMessageDescriptor("shop.Order.Line"));
        assertThat(schema.findMessage("shop.Order.AttributesEntry")).isEmpty();
        assertThat(schema.findMessage("Order")).isEmpty();
    }

    @Test
    public void testSchemaWithoutMessages()
    {
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                enum Kind {
                    PLAIN = 0;
                }
                """);
        assertThat(schema.name()).isNull();
        assertTrinoException(schema::getMessageDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema does not declare any message");
    }

    @Test
    public void testWellKnownTypesNeedNoReferences()
    {
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                import "google/protobuf/any.proto";
                import "google/protobuf/duration.proto";
                import "google/protobuf/struct.proto";
                import "google/protobuf/timestamp.proto";
                import "google/protobuf/wrappers.proto";
                message Event {
                    google.protobuf.Timestamp at = 1;
                    google.protobuf.Duration took = 2;
                    google.protobuf.Struct payload = 3;
                    google.protobuf.Any detail = 4;
                    google.protobuf.StringValue label = 5;
                }
                """);
        Descriptor event = schema.getMessageDescriptor();
        assertThat(event.findFieldByName("at").getMessageType()).isSameAs(Timestamp.getDescriptor());
        assertThat(event.findFieldByName("took").getMessageType()).isSameAs(Duration.getDescriptor());
        assertThat(event.findFieldByName("payload").getMessageType()).isSameAs(Struct.getDescriptor());
        assertThat(event.findFieldByName("detail").getMessageType()).isSameAs(Any.getDescriptor());
        assertThat(event.findFieldByName("label").getMessageType()).isSameAs(StringValue.getDescriptor());
    }

    @Test
    public void testConfluentTypesNeedNoReferences()
    {
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                import "confluent/meta.proto";
                import "confluent/type/decimal.proto";
                message Payment {
                    option (confluent.message_meta) = { doc: "A payment" };
                    confluent.type.Decimal amount = 1 [(confluent.field_meta) = { params: [{ key: "precision", value: "10" }] }];
                    confluent.Meta meta = 2;
                }
                """);
        Descriptor payment = schema.getMessageDescriptor();
        Descriptor decimal = payment.findFieldByName("amount").getMessageType();
        assertThat(decimal.getFullName()).isEqualTo("confluent.type.Decimal");
        assertThat(decimal.getFields()).extracting(FieldDescriptor::getName, FieldDescriptor::getNumber, FieldDescriptor::getType)
                .containsExactly(
                        tuple("value", 1, FieldDescriptor.Type.BYTES),
                        tuple("precision", 2, FieldDescriptor.Type.UINT32),
                        tuple("scale", 3, FieldDescriptor.Type.INT32));
        Descriptor meta = payment.findFieldByName("meta").getMessageType();
        assertThat(meta.getFullName()).isEqualTo("confluent.Meta");
        assertThat(meta.findFieldByName("params").isMapField()).isTrue();
    }

    @Test
    public void testKnownTypesOfConfluentSerializersNeedNoReferences()
    {
        // Confluent serializers register schemas without references for their known types, so every
        // one of them must resolve without a reference ...
        for (String knownType : ProtobufSchema.knownTypes()) {
            FileDescriptor file = schema(
                    """
                    syntax = "proto3";
                    import "%s";
                    message Event {}
                    """.formatted(knownType)).getFileDescriptor();
            assertThat(file.getDependencies()).as(knownType).extracting(FileDescriptor::getName).containsExactly(knownType);
        }

        // ... and nothing else does: the serializers register references for every other import
        ConfluentProtobufSchema schema = schema(
                """
                syntax = "proto3";
                import "google/type/localized_text.proto";
                message Event {
                    google.type.LocalizedText title = 1;
                }
                """);
        assertTrinoException(schema::getFileDescriptor, INVALID_PROTO_FILE, "Import 'google/type/localized_text.proto' of protobuf schema 'schema.proto' could not be resolved");
    }

    @Test
    public void testReferenceTakesPrecedenceOverConfluentType()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("decimal-value", schema(client,
                """
                syntax = "proto3";
                package confluent.type;
                message Decimal {
                    string text = 1;
                }
                """));
        ConfluentProtobufSchema schema = schema(
                client,
                """
                syntax = "proto3";
                import "confluent/type/decimal.proto";
                message Payment {
                    confluent.type.Decimal amount = 1;
                }
                """,
                new SchemaReference("confluent/type/decimal.proto", "decimal-value", 1));
        assertThat(schema.getMessageDescriptor().findFieldByName("amount").getMessageType().findFieldByName("text")).isNotNull();
    }

    @Test
    public void testReference()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        ConfluentProtobufSchema order = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1));

        Descriptor descriptor = order.getMessageDescriptor();
        assertThat(descriptor.getName()).isEqualTo("Order");
        assertThat(descriptor.findFieldByName("shipping_address").getMessageType().getFullName()).isEqualTo("base.Address");
        assertThat(descriptor.getFile().getDependencies()).extracting(FileDescriptor::getName).containsExactly("address.proto");
        assertThat(order.getFileDescriptor()).isSameAs(order.getFileDescriptor());
    }

    @Test
    public void testTransitiveReferences()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        client.register("order-value", schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1)));
        ConfluentProtobufSchema shipment = schema(
                client,
                """
                syntax = "proto3";
                import "order.proto";
                message Shipment {
                    repeated Order orders = 1;
                }
                """,
                new SchemaReference("order.proto", "order-value", 1));

        Descriptor descriptor = shipment.getMessageDescriptor();
        Descriptor orderDescriptor = descriptor.findFieldByName("orders").getMessageType();
        assertThat(orderDescriptor.getFullName()).isEqualTo("Order");
        assertThat(orderDescriptor.findFieldByName("shipping_address").getMessageType().getFullName()).isEqualTo("base.Address");
    }

    @Test
    public void testReferenceUsedTwiceIsBuiltOnce()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        client.register("order-value", schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1)));
        ConfluentProtobufSchema schema = schema(
                client,
                """
                syntax = "proto3";
                import "address.proto";
                import "order.proto";
                message Delivery {
                    Order order = 1;
                    base.Address destination = 2;
                }
                """,
                new SchemaReference("address.proto", "address-value", 1),
                new SchemaReference("order.proto", "order-value", 1));

        Descriptor delivery = schema.getMessageDescriptor();
        Descriptor viaOrder = delivery.findFieldByName("order").getMessageType().findFieldByName("shipping_address").getMessageType();
        Descriptor direct = delivery.findFieldByName("destination").getMessageType();
        assertThat(direct).isSameAs(viaOrder);
    }

    @Test
    public void testReferenceToLatestVersion()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        client.register("address-value", schema(client,
                """
                syntax = "proto3";
                package base;
                message Address {
                    string city = 1;
                    string country = 2;
                }
                """));
        ConfluentProtobufSchema pinned = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1));
        ConfluentProtobufSchema latest = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", -1));

        assertThat(pinned.getMessageDescriptor().findFieldByName("shipping_address").getMessageType().getFields()).hasSize(1);
        assertThat(latest.getMessageDescriptor().findFieldByName("shipping_address").getMessageType().getFields()).hasSize(2);
    }

    @Test
    public void testDanglingReference()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        ConfluentProtobufSchema unknownSubject = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1));
        assertTrinoException(unknownSubject::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'address.proto' points to address-value:1, which does not exist in the schema registry");

        client.register("address-value", schema(client, ADDRESS_PROTO));
        ConfluentProtobufSchema unknownVersion = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 7));
        assertTrinoException(unknownVersion::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'address.proto' points to address-value:7, which does not exist in the schema registry");
    }

    @Test
    public void testFailedResolutionIsRetried()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        ConfluentProtobufSchema order = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1));
        assertThatThrownBy(order::getFileDescriptor).isInstanceOf(TrinoException.class);

        client.register("address-value", schema(client, ADDRESS_PROTO));
        assertThat(order.getMessageDescriptor().getName()).isEqualTo("Order");
    }

    @Test
    public void testReferenceCycle()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        // the mock registry does not compile schemas, so a cycle can be registered
        client.register("a-value", schema(client, "syntax = \"proto3\"; import \"b.proto\"; message A { B b = 1; }", new SchemaReference("b.proto", "b-value", 1)));
        client.register("b-value", schema(client, "syntax = \"proto3\"; import \"a.proto\"; message B { A a = 1; }", new SchemaReference("a.proto", "a-value", 1)));

        ConfluentProtobufSchema root = schema(client, "syntax = \"proto3\"; import \"a.proto\"; message Root { A a = 1; }", new SchemaReference("a.proto", "a-value", 1));
        assertTrinoException(root::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema references form a cycle: a-value:1 -> b-value:1 -> a-value:1");
    }

    @Test
    public void testSelfReference()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("a-value", schema(client, "syntax = \"proto3\"; message A { A a = 1; }", new SchemaReference("a.proto", "a-value", 1)));
        ConfluentProtobufSchema root = schema(client, "syntax = \"proto3\"; import \"a.proto\"; message Root { A a = 1; }", new SchemaReference("a.proto", "a-value", 1));
        assertTrinoException(root::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema references form a cycle: a-value:1 -> a-value:1");
    }

    @Test
    public void testConflictingReferenceNames()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        client.register("other-address-value", schema(client,
                """
                syntax = "proto3";
                package other;
                message Address {
                    string street = 1;
                }
                """));
        client.register("order-value", schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1)));

        ConfluentProtobufSchema direct = schema(
                client,
                "syntax = \"proto3\"; import \"address.proto\"; message Root { base.Address a = 1; }",
                new SchemaReference("address.proto", "address-value", 1),
                new SchemaReference("address.proto", "other-address-value", 1));
        assertTrinoException(direct::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'address.proto' points to both address-value:1 and other-address-value:1");

        ConfluentProtobufSchema transitive = schema(
                client,
                "syntax = \"proto3\"; import \"order.proto\"; message Root { Order order = 1; }",
                new SchemaReference("address.proto", "other-address-value", 1),
                new SchemaReference("order.proto", "order-value", 1));
        assertTrinoException(transitive::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'address.proto' points to both other-address-value:1 and address-value:1");
    }

    @Test
    public void testReferenceToNonProtobufSchema()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", new AvroSchema(SchemaBuilder.record("Address").fields().name("city").type().stringType().noDefault().endRecord()));
        ConfluentProtobufSchema order = schema(client, ORDER_PROTO, new SchemaReference("address.proto", "address-value", 1));
        assertTrinoException(order::getFileDescriptor, KAFKA_SCHEMA_ERROR, "Protobuf schema reference 'address.proto' points to address-value:1, which is a AVRO schema");
    }

    @Test
    public void testIncompleteReference()
    {
        MockSchemaRegistryClient client = createClient();
        ConfluentProtobufSchema order = schema(client, ORDER_PROTO, new SchemaReference("address.proto", null, 1));
        assertThatThrownBy(order::getFileDescriptor)
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Protobuf schema has an incomplete reference");
    }

    @Test
    public void testReferenceNamedLikeTheRootFile()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register("address-value", schema(client, ADDRESS_PROTO));
        ConfluentProtobufSchema order = schema(
                client,
                "syntax = \"proto3\"; import \"schema.proto\"; message Order { base.Address address = 1; }",
                new SchemaReference("schema.proto", "address-value", 1));

        Descriptor descriptor = order.getMessageDescriptor();
        assertThat(descriptor.getFile().getName()).isEqualTo("_schema.proto");
        assertThat(descriptor.findFieldByName("address").getMessageType().getFile().getName()).isEqualTo("schema.proto");
    }

    @Test
    public void testUnresolvableImport()
    {
        ConfluentProtobufSchema order = schema(ORDER_PROTO);
        assertTrinoException(order::getFileDescriptor, INVALID_PROTO_FILE, "Import 'address.proto' of protobuf schema 'schema.proto' could not be resolved");
    }

    @Test
    public void testInvalidSchema()
    {
        ConfluentProtobufSchema unparsable = schema("message {");
        assertThatThrownBy(unparsable::getFileDescriptor)
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Failed to parse protobuf schema 'schema.proto'");

        ConfluentProtobufSchema unknownType = schema("syntax = \"proto3\"; message A { Missing missing = 1; }");
        assertThatThrownBy(unknownType::getFileDescriptor)
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'schema.proto'")
                .hasMessageContaining("Missing");
    }

    @Test
    public void testEquality()
    {
        List<SchemaReference> references = ImmutableList.of(new SchemaReference("address.proto", "address-value", 1));
        ConfluentProtobufSchema schema = new ConfluentProtobufSchema(ORDER_PROTO, references, 1, null, null, null);
        assertThat(schema).isEqualTo(new ConfluentProtobufSchema(ORDER_PROTO, references, null, null, null, null));
        assertThat(schema).hasSameHashCodeAs(new ConfluentProtobufSchema(ORDER_PROTO, references, 2, null, null, null));
        assertThat(schema).isNotEqualTo(new ConfluentProtobufSchema(ORDER_PROTO, ImmutableList.of(), 1, null, null, null));
        assertThat(schema).isNotEqualTo(new ConfluentProtobufSchema(ADDRESS_PROTO, references, 1, null, null, null));
        assertThat(schema).isNotEqualTo(new ConfluentProtobufSchema(ORDER_PROTO, references, 1, new Metadata(Map.of(), Map.of("owner", "trino"), null), null, null));
    }

    @Test
    public void testCopy()
    {
        Metadata metadata = new Metadata(Map.of(), Map.of("owner", "trino"), null);
        RuleSet ruleSet = new RuleSet(ImmutableList.of(), ImmutableList.of());
        ConfluentProtobufSchema schema = new ConfluentProtobufSchema(ADDRESS_PROTO, ImmutableList.of(), 1, null, null, null);

        assertThat(schema.copy()).isEqualTo(schema);
        assertThat(schema.copy(5).version()).isEqualTo(5);
        ParsedSchema withMetadata = schema.copy(metadata, ruleSet);
        assertThat(withMetadata.metadata()).isSameAs(metadata);
        assertThat(withMetadata.ruleSet()).isSameAs(ruleSet);
        assertThat(withMetadata.version()).isEqualTo(1);
        assertThat(((ConfluentProtobufSchema) withMetadata).getMessageDescriptor().getName()).isEqualTo("Address");
    }

    @Test
    public void testProviderCanOnlyBeConfiguredOnce()
    {
        MockSchemaRegistryClient client = createClient();
        ConfluentProtobufSchemaProvider provider = new ConfluentProtobufSchemaProvider();
        provider.configure(Map.of(ConfluentProtobufSchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, client));
        assertThatThrownBy(() -> provider.configure(Map.of(ConfluentProtobufSchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, client)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ConfluentProtobufSchemaProvider is already configured");
        assertThatThrownBy(() -> new ConfluentProtobufSchemaProvider().configure(Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a SchemaVersionFetcher");
    }

    private static MockSchemaRegistryClient createClient()
    {
        return new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
    }

    private static ConfluentProtobufSchema schema(String schema)
    {
        return new ConfluentProtobufSchema(schema, ImmutableList.of(), null);
    }

    private static ConfluentProtobufSchema schema(MockSchemaRegistryClient client, String schema, SchemaReference... references)
    {
        return new ConfluentProtobufSchema(schema, ImmutableList.copyOf(references), client);
    }

    private static void assertTrinoException(ThrowingCallable callable, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(callable::call)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()))
                .hasMessage(message);
    }

    private interface ThrowingCallable
    {
        void call()
                throws Exception;
    }
}
