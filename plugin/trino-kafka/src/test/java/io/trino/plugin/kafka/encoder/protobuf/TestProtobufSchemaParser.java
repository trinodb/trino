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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.ProtobufAnySupportConfig;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufSchema;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.testing.TestingConnectorSession;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtobufSchemaParser
{
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));

    @Test
    public void testScalarAndStructuralTypes()
    {
        KafkaTopicFieldGroup fieldGroup = parse(
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                message schema {
                    string string_column = 1;
                    uint32 integer_column = 2;
                    int64 long_column = 3;
                    double double_column = 4;
                    float float_column = 5;
                    bool boolean_column = 6;
                    Number number_column = 7;
                    google.protobuf.Timestamp timestamp_column = 8;
                    bytes bytes_column = 9;
                    repeated string list = 10;
                    map<string, int64> map = 11;
                    message Row {
                        string name = 1;
                        repeated Child children = 2;
                    }
                    message Child {
                        int64 id = 1;
                    }
                    Row row = 12;
                }
                enum Number {
                    ZERO = 0;
                }
                """,
                false);

        assertThat(fieldGroup.dataFormat()).isEqualTo("protobuf");
        assertThat(fieldGroup.dataSchema()).contains("schema-registry-id:1;message=schema");
        assertThat(fieldGroup.subject()).contains("topic-value");
        assertThat(fieldGroup.fields()).extracting(KafkaTopicFieldDescription::name).containsExactly(
                "string_column",
                "integer_column",
                "long_column",
                "double_column",
                "float_column",
                "boolean_column",
                "number_column",
                "timestamp_column",
                "bytes_column",
                "list",
                "map",
                "row");
        assertThat(fieldGroup.fields()).extracting(KafkaTopicFieldDescription::type).containsExactly(
                createUnboundedVarcharType(),
                INTEGER,
                BIGINT,
                DOUBLE,
                REAL,
                BOOLEAN,
                createUnboundedVarcharType(),
                createTimestampType(6),
                VARBINARY,
                new ArrayType(createUnboundedVarcharType()),
                new MapType(createUnboundedVarcharType(), BIGINT, TESTING_TYPE_MANAGER.getTypeOperators()),
                RowType.rowType(
                        RowType.field("name", createUnboundedVarcharType()),
                        RowType.field("children", new ArrayType(RowType.rowType(RowType.field("id", BIGINT))))));
    }

    @Test
    public void testOneofBecomesJsonColumn()
    {
        KafkaTopicFieldGroup fieldGroup = parse(
                """
                syntax = "proto3";
                message schema {
                    string id = 1;
                    oneof payload {
                        string text = 2;
                        int64 number = 3;
                    }
                    optional string note = 4;
                }
                """,
                false);

        // the fields of a oneof are replaced by one JSON column; a proto3 optional field is an ordinary column
        assertThat(fieldGroup.fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("id", "note", "payload");
        assertThat(fieldGroup.fields()).extracting(KafkaTopicFieldDescription::type).containsExactly(createUnboundedVarcharType(), createUnboundedVarcharType(), JSON_TYPE);
    }

    @Test
    public void testAnySupport()
    {
        String schema =
                """
                syntax = "proto3";
                import "google/protobuf/any.proto";
                message schema {
                    google.protobuf.Any detail = 1;
                }
                """;
        assertThat(parse(schema, true).fields()).extracting(KafkaTopicFieldDescription::type).containsExactly(JSON_TYPE);
        assertThat(parse(schema, false).fields()).extracting(KafkaTopicFieldDescription::type).containsExactly(
                RowType.rowType(RowType.field("type_url", createUnboundedVarcharType()), RowType.field("value", VARBINARY)));
    }

    @Test
    public void testRecursiveSchemaIsRejected()
    {
        assertThatThrownBy(() -> parse(
                """
                syntax = "proto3";
                message schema {
                    Node root = 1;
                }
                message Node {
                    map<string, Node> children = 1;
                }
                """,
                false))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Protobuf schema containing fields with self-reference are not supported because they cannot be mapped to a Trino type: " +
                        "schema.root: Node > Node.children: Node.ChildrenEntry > Node.ChildrenEntry.value: Node");
    }

    @Test
    public void testFirstMessageIsUsed()
    {
        KafkaTopicFieldGroup fieldGroup = parse(
                """
                syntax = "proto3";
                message Envelope {
                    string id = 1;
                }
                message schema {
                    string ignored = 1;
                }
                """,
                false);
        assertThat(fieldGroup.fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("id");
    }

    @Test
    public void testSubjectSelectsTheMessage()
    {
        String schema =
                """
                syntax = "proto3";
                package shop;
                message Header {
                    string name = 1;
                }
                message Order {
                    int64 id = 1;
                    string customer = 2;
                    message Line {
                        string sku = 1;
                        int32 quantity = 2;
                    }
                }
                """;
        assertThat(parse("orders-value", schema).fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("name");
        assertThat(parse("shop.Order", schema).fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("id", "customer");
        assertThat(parse("orders-shop.Order.Line", schema).fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("sku", "quantity");
        assertThat(parse("orders-shop.Missing", schema).fields()).extracting(KafkaTopicFieldDescription::name).containsExactly("name");
    }

    @Test
    public void testNonProtobufSchemaIsRejected()
    {
        ProtobufSchemaParser parser = new ProtobufSchemaParser(TESTING_TYPE_MANAGER, new ProtobufAnySupportConfig());
        AvroSchema avroSchema = new AvroSchema(SchemaBuilder.record("schema").fields().name("id").type().stringType().noDefault().endRecord());
        assertThatThrownBy(() -> parser.parse(TestingConnectorSession.SESSION, "topic-value", 1, avroSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unexpected schema type for subject 'topic-value': AVRO");
    }

    private static KafkaTopicFieldGroup parse(String schema, boolean anySupportEnabled)
    {
        ProtobufSchemaParser parser = new ProtobufSchemaParser(TESTING_TYPE_MANAGER, new ProtobufAnySupportConfig().setProtobufAnySupportEnabled(anySupportEnabled));
        return parser.parse(TestingConnectorSession.SESSION, "topic-value", 1, new ConfluentProtobufSchema(schema, ImmutableList.of(), null));
    }

    private static KafkaTopicFieldGroup parse(String subject, String schema)
    {
        ProtobufSchemaParser parser = new ProtobufSchemaParser(TESTING_TYPE_MANAGER, new ProtobufAnySupportConfig());
        return parser.parse(TestingConnectorSession.SESSION, subject, 1, new ConfluentProtobufSchema(schema, ImmutableList.of(), null));
    }
}
