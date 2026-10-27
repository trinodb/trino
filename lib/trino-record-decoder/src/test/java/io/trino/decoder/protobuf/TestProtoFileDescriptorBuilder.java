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
package io.trino.decoder.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.Value;
import com.google.type.Date;
import com.google.type.Money;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestProtoFileDescriptorBuilder
{
    private static final ProtoFileDescriptorBuilder.ImportResolver NO_IMPORTS = _ -> Optional.empty();

    @Test
    public void testScalarsLabelsAndEnums()
    {
        FileDescriptor file = build("scalars.proto",
                """
                syntax = "proto2";
                package test;
                enum Color {
                  option allow_alias = true;
                  RED = 0;
                  CRIMSON = 0;
                  BLUE = 1;
                }
                message Scalars {
                  required int32 a = 1;
                  optional string b = 2 [default = "x"];
                  repeated sint64 c = 3 [packed = true];
                  optional Color color = 4 [default = BLUE];
                  optional bytes d = 5;
                  optional double e = 6 [json_name = "ee"];
                }
                """);
        assertThat(file.toProto().getSyntax()).isEqualTo("proto2");
        assertThat(file.getPackage()).isEqualTo("test");

        Descriptor scalars = file.findMessageTypeByName("Scalars");
        assertThat(scalars.findFieldByName("a").isRequired()).isTrue();
        assertThat(scalars.findFieldByName("a").getType()).isEqualTo(FieldDescriptor.Type.INT32);
        assertThat(scalars.findFieldByName("b").getDefaultValue()).isEqualTo("x");
        FieldDescriptor packed = scalars.findFieldByName("c");
        assertThat(packed.isRepeated()).isTrue();
        assertThat(packed.isPacked()).isTrue();
        assertThat(packed.getType()).isEqualTo(FieldDescriptor.Type.SINT64);
        FieldDescriptor color = scalars.findFieldByName("color");
        assertThat(color.getJavaType()).isEqualTo(JavaType.ENUM);
        assertThat(color.getEnumType().findValueByName("CRIMSON").getNumber()).isEqualTo(0);
        assertThat(color.getDefaultValue()).isEqualTo(color.getEnumType().findValueByName("BLUE"));
        assertThat(scalars.findFieldByName("d").getType()).isEqualTo(FieldDescriptor.Type.BYTES);
        assertThat(scalars.findFieldByName("e").getJsonName()).isEqualTo("ee");
    }

    @Test
    public void testOneofsNestedTypesAndMaps()
    {
        FileDescriptor file = build("nested.proto",
                """
                syntax = "proto3";
                package test;
                message Outer {
                  message Inner {
                    string value = 1;
                    Deep deep = 2;
                    message Deep {
                      int32 depth = 1;
                    }
                  }
                  enum Kind {
                    UNKNOWN = 0;
                    KNOWN = 1;
                  }
                  string name = 1;
                  oneof choice {
                    Inner inner = 2;
                    Inner.Deep deep = 3;
                    Kind kind = 4;
                  }
                  map<string, Inner> by_name = 5;
                  map<int64, string> labels = 6;
                  repeated Inner inners = 7;
                  optional int32 maybe = 8;
                  message Later {
                    Outer outer = 1;
                  }
                }
                message Second {
                  Outer.Inner.Deep deep = 1;
                }
                """);
        assertThat(file.toProto().getSyntax()).isEqualTo("proto3");
        Descriptor outer = file.findMessageTypeByName("Outer");
        assertThat(outer.getFields().stream().map(FieldDescriptor::getName))
                .containsExactly("name", "by_name", "labels", "inners", "maybe", "inner", "deep", "kind");

        // declared nested messages keep their source order, map entries follow
        assertThat(outer.getNestedTypes().stream().map(Descriptor::getName))
                .containsExactly("Inner", "Later", "ByNameEntry", "LabelsEntry");
        assertThat(outer.findNestedTypeByName("ByNameEntry").getOptions().getMapEntry()).isTrue();
        assertThat(outer.findNestedTypeByName("Later").getOptions().getMapEntry()).isFalse();

        assertThat(outer.getRealOneofs()).hasSize(1);
        assertThat(outer.getRealOneofs().getFirst().getName()).isEqualTo("choice");
        assertThat(outer.findFieldByName("inner").getContainingOneof().getName()).isEqualTo("choice");
        assertThat(outer.findFieldByName("deep").getMessageType().getFullName()).isEqualTo("test.Outer.Inner.Deep");
        assertThat(outer.findFieldByName("kind").getJavaType()).isEqualTo(JavaType.ENUM);

        FieldDescriptor byName = outer.findFieldByName("by_name");
        assertThat(byName.isMapField()).isTrue();
        assertThat(byName.getMessageType().findFieldByName("value").getMessageType().getName()).isEqualTo("Inner");
        assertThat(outer.findFieldByName("labels").getMessageType().findFieldByName("key").getType()).isEqualTo(FieldDescriptor.Type.INT64);
        assertThat(outer.findFieldByName("inners").isRepeated()).isTrue();

        FieldDescriptor maybe = outer.findFieldByName("maybe");
        assertThat(maybe.toProto().getProto3Optional()).isTrue();
        assertThat(maybe.getContainingOneof().getName()).isEqualTo("_maybe");
        assertThat(maybe.getContainingOneof().getFieldCount()).isEqualTo(1);
        assertThat(outer.getOneofs()).hasSize(2);

        // types referenced before their declaration and across messages resolve
        assertThat(outer.findNestedTypeByName("Later").findFieldByName("outer").getMessageType()).isSameAs(outer);
        assertThat(file.findMessageTypeByName("Second").findFieldByName("deep").getMessageType().getFullName()).isEqualTo("test.Outer.Inner.Deep");
    }

    @Test
    public void testWellKnownTypesLinkAgainstBundledDescriptors()
    {
        FileDescriptor file = build("wkt.proto",
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                import "google/protobuf/any.proto";
                import "google/protobuf/struct.proto";
                message Event {
                  google.protobuf.Timestamp at = 1;
                  google.protobuf.Any payload = 2;
                  google.protobuf.Struct extra = 3;
                }
                """);
        Descriptor event = file.findMessageTypeByName("Event");
        assertThat(event.findFieldByName("at").getMessageType()).isSameAs(Timestamp.getDescriptor());
        assertThat(file.getDependencies()).contains(TimestampProto.getDescriptor());
    }

    @Test
    public void testGoogleCommonTypesLinkAgainstBundledDescriptors()
    {
        // schema registry serializers register these imports without references, so they must never reach the resolver
        FileDescriptor file = build(
                "common.proto",
                """
                syntax = "proto3";
                import "google/type/date.proto";
                import "google/type/money.proto";
                import "google/type/decimal.proto";
                import "google/type/color.proto";
                message Invoice {
                  google.type.Date due = 1;
                  google.type.Money total = 2;
                  google.type.Decimal rate = 3;
                  google.type.Color accent = 4;
                }
                """,
                importPath -> {
                    throw new AssertionError("Unexpected import resolution: " + importPath);
                });
        Descriptor invoice = file.findMessageTypeByName("Invoice");
        assertThat(invoice.findFieldByName("due").getMessageType()).isSameAs(Date.getDescriptor());
        assertThat(invoice.findFieldByName("total").getMessageType()).isSameAs(Money.getDescriptor());
        assertThat(invoice.findFieldByName("rate").getMessageType().getFullName()).isEqualTo("google.type.Decimal");
        // color.proto itself imports google/protobuf/wrappers.proto
        assertThat(invoice.findFieldByName("accent").getMessageType().findFieldByName("alpha").getMessageType().getFullName()).isEqualTo("google.protobuf.FloatValue");
    }

    @Test
    public void testExplicitMapEntryMessages()
    {
        // the form of a map field in schema text generated from a descriptor: fully qualified type
        // names and an explicit entry message, in the position protoc gives it
        FileDescriptor file = build("generated.proto",
                """
                syntax = "proto3";
                package test;

                message Holder {
                  repeated .test.Holder.AttributesEntry attributes = 1;
                  repeated .test.Holder.ValuesEntry values = 2;
                  repeated .test.Holder.NotAnEntry plain = 3;

                  message AttributesEntry {
                    option map_entry = true;

                    string key = 1;
                    string value = 2;
                  }
                  message ValuesEntry {
                    option map_entry = true;

                    int64 key = 1;
                    .test.Holder.Value value = 2;
                  }
                  message NotAnEntry {
                    option map_entry = false;
                    option deprecated = true;

                    string key = 1;
                    string value = 2;
                  }
                  message Value {
                    string text = 1;
                  }
                }
                """);
        Descriptor holder = file.findMessageTypeByName("Holder");
        assertThat(holder.findFieldByName("attributes").isMapField()).isTrue();
        FieldDescriptor values = holder.findFieldByName("values");
        assertThat(values.isMapField()).isTrue();
        assertThat(values.getMessageType().findFieldByName("value").getMessageType()).isSameAs(holder.findNestedTypeByName("Value"));
        assertThat(holder.findFieldByName("plain").isMapField()).isFalse();
        assertThat(holder.findNestedTypeByName("NotAnEntry").getOptions().getDeprecated()).isTrue();
        // explicit entry messages keep their position among the nested messages
        assertThat(holder.getNestedTypes()).extracting(Descriptor::getName).containsExactly("AttributesEntry", "ValuesEntry", "NotAnEntry", "Value");
    }

    @Test
    public void testMalformedExplicitMapEntryIsRejected()
    {
        // decoding and encoding a map rely on the entry message having the shape protoc gives it
        for (String entry : ImmutableList.of(
                "string key = 1; string value = 2; string extra = 3;",
                "string key = 1;",
                "string name = 1; string value = 2;",
                "string key = 2; string value = 1;",
                "repeated string key = 1; string value = 2;",
                "string key = 1; string value = 2; message Nested {}",
                "string key = 1; string value = 2; enum Kind { UNKNOWN = 0; }",
                "oneof variant { string key = 1; string value = 2; }")) {
            assertThatThrownBy(() -> build("entries.proto",
                    """
                    syntax = "proto3";
                    message Holder {
                      repeated .Holder.AttributesEntry attributes = 1;
                      message AttributesEntry {
                        option map_entry = true;
                        %s
                      }
                    }
                    """.formatted(entry)))
                    .as(entry)
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Message 'AttributesEntry' in protobuf schema 'entries.proto' is declared as a map entry but does not consist of the fields key = 1 and value = 2")
                    .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(INVALID_PROTO_FILE.toErrorCode()));
        }
    }

    @Test
    public void testImportsResolveThroughResolverAndAreBuiltOnce()
    {
        Map<String, String> files = ImmutableMap.of(
                "common/money.proto",
                """
                syntax = "proto3";
                package common;
                message Money {
                  string currency = 1;
                  int64 units = 2;
                }
                """,
                "orders/line.proto",
                """
                syntax = "proto3";
                package orders;
                import "common/money.proto";
                message Line {
                  common.Money price = 1;
                }
                """);
        FileDescriptor file = build(
                "orders/order.proto",
                """
                syntax = "proto3";
                package orders;
                import "common/money.proto";
                import "orders/line.proto";
                message Order {
                  repeated Line lines = 1;
                  .common.Money total = 2;
                }
                """,
                importPath -> Optional.ofNullable(files.get(importPath)));
        Descriptor order = file.findMessageTypeByName("Order");
        Descriptor money = order.findFieldByName("total").getMessageType();
        assertThat(money.getFullName()).isEqualTo("common.Money");
        // the diamond import is a single descriptor instance
        assertThat(order.findFieldByName("lines").getMessageType().findFieldByName("price").getMessageType()).isSameAs(money);
        assertThat(file.getDependencies().stream().map(FileDescriptor::getName)).containsExactly("common/money.proto", "orders/line.proto");
    }

    @Test
    public void testPublicImportsAreTransitive()
    {
        Map<String, String> files = ImmutableMap.of(
                "a.proto",
                """
                syntax = "proto3";
                message A {
                  int32 x = 1;
                }
                """,
                "b.proto",
                """
                syntax = "proto3";
                import public "a.proto";
                """);
        FileDescriptor file = build(
                "c.proto",
                """
                syntax = "proto3";
                import "b.proto";
                message C {
                  A a = 1;
                }
                """,
                importPath -> Optional.ofNullable(files.get(importPath)));
        assertThat(file.findMessageTypeByName("C").findFieldByName("a").getMessageType().getFullName()).isEqualTo("A");
    }

    @Test
    public void testUnresolvedImport()
    {
        assertThatThrownBy(() -> build("x.proto",
                """
                syntax = "proto3";
                import "missing.proto";
                message X {}
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Import 'missing.proto' of protobuf schema 'x.proto' could not be resolved")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(INVALID_PROTO_FILE.toErrorCode()));
    }

    @Test
    public void testImportCycle()
    {
        Map<String, String> files = ImmutableMap.of(
                "a.proto",
                """
                syntax = "proto3";
                import "b.proto";
                """,
                "b.proto",
                """
                syntax = "proto3";
                import "a.proto";
                """);
        assertThatThrownBy(() -> build("a.proto", files.get("a.proto"), importPath -> Optional.ofNullable(files.get(importPath))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Import cycle in protobuf schema: a.proto -> b.proto -> a.proto");
    }

    @Test
    public void testUnknownTypeAndParseErrorsFailLoudly()
    {
        assertThatThrownBy(() -> build("x.proto",
                """
                syntax = "proto3";
                message X {
                  Missing m = 1;
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'x.proto': ")
                .hasMessageContaining("\"Missing\" is not defined");

        assertThatThrownBy(() -> build("x.proto", "message X { int32 = 1; }"))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Failed to parse protobuf schema 'x.proto': ");
    }

    @Test
    public void testMissingSyntaxIsProto2()
    {
        FileDescriptor file = build("legacy.proto",
                """
                message Legacy {
                  optional string name = 1;
                  repeated int32 values = 2;
                }
                """);
        assertThat(file.toProto().hasSyntax()).isFalse();
        FieldDescriptor name = file.findMessageTypeByName("Legacy").findFieldByName("name");
        assertThat(name.hasPresence()).isTrue();
        assertThat(file.findMessageTypeByName("Legacy").findFieldByName("values").isPacked()).isFalse();
    }

    @Test
    public void testProto3FieldsHaveNoPresenceUnlessOptional()
    {
        FileDescriptor file = build("presence.proto",
                """
                syntax = "proto3";
                message Presence {
                  string implicit = 1;
                  optional string explicit = 2;
                  repeated int32 values = 3;
                  Presence nested = 4;
                }
                """);
        Descriptor presence = file.findMessageTypeByName("Presence");
        assertThat(presence.findFieldByName("implicit").hasPresence()).isFalse();
        assertThat(presence.findFieldByName("explicit").hasPresence()).isTrue();
        assertThat(presence.findFieldByName("explicit").getContainingOneof().getName()).isEqualTo("_explicit");
        assertThat(presence.getRealOneofs()).isEmpty();
        assertThat(presence.findFieldByName("values").isPacked()).isTrue();
        assertThat(presence.findFieldByName("nested").hasPresence()).isTrue();

        // a proto3 implicit-presence field set to its default value is not serialized
        DynamicMessage message = DynamicMessage.newBuilder(presence)
                .setField(presence.findFieldByName("implicit"), "")
                .setField(presence.findFieldByName("explicit"), "")
                .build();
        assertThat(message.toByteArray()).containsExactly(0x12, 0x00);
    }

    @Test
    public void testDeclarationsWithoutDescriptorsAreIgnored()
    {
        FileDescriptor file = build("ignored.proto",
                """
                syntax = "proto2";
                package test;
                import "google/protobuf/descriptor.proto";
                option java_package = "com.example";
                option (my_file_option) = "x";
                extend google.protobuf.FieldOptions {
                  optional string my_option = 51234;
                }
                message Settings {
                  option deprecated = true;
                  option (my_message_option) = 7;
                  reserved 2, 15 to 20;
                  reserved "old_name", "older_name";
                  extensions 100 to 199;
                  optional string name = 1 [(my_option) = "value", deprecated = true];
                  optional int32 count = 3 [(my_option) = "value"];
                }
                service Api {
                  rpc Get (Settings) returns (Settings);
                }
                """);
        Descriptor settings = file.findMessageTypeByName("Settings");
        assertThat(settings.getFields()).extracting(FieldDescriptor::getName).containsExactly("name", "count");
        assertThat(settings.findFieldByName("name").getOptions().getDeprecated()).isTrue();
        assertThat(settings.findFieldByName("count").getOptions().getDeprecated()).isFalse();
        assertThat(file.getServices()).isEmpty();
        assertThat(file.getExtensions()).isEmpty();
    }

    @Test
    public void testTypeNameResolution()
    {
        FileDescriptor file = build("names.proto",
                """
                syntax = "proto3";
                package outer.inner;
                message Outer {
                  enum Kind {
                    NONE = 0;
                    SOME = 1;
                  }
                  message Nested {
                    Kind kind = 1;
                    Outer parent = 2;
                    .outer.inner.Outer qualified_parent = 3;
                    Sibling sibling = 4;
                    outer.inner.Sibling.Deep deep = 5;
                  }
                  Nested nested = 1;
                  Outer.Nested qualified_nested = 2;
                  Sibling.Deep deep = 3;
                }
                message Sibling {
                  message Deep {
                    Outer.Kind kind = 1;
                  }
                  Outer.Nested nested = 1;
                }
                """);
        Descriptor outer = file.findMessageTypeByName("Outer");
        Descriptor nested = outer.findNestedTypeByName("Nested");
        Descriptor sibling = file.findMessageTypeByName("Sibling");
        Descriptor deep = sibling.findNestedTypeByName("Deep");

        assertThat(nested.findFieldByName("kind").getEnumType()).isSameAs(outer.findEnumTypeByName("Kind"));
        assertThat(nested.findFieldByName("parent").getMessageType()).isSameAs(outer);
        assertThat(nested.findFieldByName("qualified_parent").getMessageType()).isSameAs(outer);
        assertThat(nested.findFieldByName("sibling").getMessageType()).isSameAs(sibling);
        assertThat(nested.findFieldByName("deep").getMessageType()).isSameAs(deep);
        assertThat(outer.findFieldByName("nested").getMessageType()).isSameAs(nested);
        assertThat(outer.findFieldByName("qualified_nested").getMessageType()).isSameAs(nested);
        assertThat(outer.findFieldByName("deep").getMessageType()).isSameAs(deep);
        assertThat(deep.findFieldByName("kind").getEnumType()).isSameAs(outer.findEnumTypeByName("Kind"));
        assertThat(sibling.findFieldByName("nested").getMessageType()).isSameAs(nested);
        assertThat(deep.getFullName()).isEqualTo("outer.inner.Sibling.Deep");
    }

    @Test
    public void testTypesFromImportedPackages()
    {
        Map<String, String> imports = ImmutableMap.of(
                "common/address.proto",
                """
                syntax = "proto3";
                package common;
                message Address {
                  string city = 1;
                }
                enum Region {
                  NORTH = 0;
                }
                """,
                "common/person.proto",
                """
                syntax = "proto3";
                package common;
                import "common/address.proto";
                message Person {
                  Address address = 1;
                  Region region = 2;
                }
                """);
        FileDescriptor file = build(
                "order.proto",
                """
                syntax = "proto3";
                package shop;
                import "common/person.proto";
                import "common/address.proto";
                message Order {
                  common.Person customer = 1;
                  common.Address destination = 2;
                  .common.Region region = 3;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath)));
        Descriptor order = file.findMessageTypeByName("Order");
        Descriptor person = order.findFieldByName("customer").getMessageType();
        assertThat(person.getFullName()).isEqualTo("common.Person");
        // both files see the same Address descriptor
        assertThat(order.findFieldByName("destination").getMessageType()).isSameAs(person.findFieldByName("address").getMessageType());
        assertThat(order.findFieldByName("region").getEnumType()).isSameAs(person.findFieldByName("region").getEnumType());
        assertThat(file.getDependencies()).extracting(FileDescriptor::getName).containsExactly("common/person.proto", "common/address.proto");
    }

    @Test
    public void testTypeFromTransitiveImportIsNotVisible()
    {
        Map<String, String> imports = ImmutableMap.of(
                "address.proto",
                """
                syntax = "proto3";
                message Address {
                  string city = 1;
                }
                """,
                "person.proto",
                """
                syntax = "proto3";
                import "address.proto";
                message Person {
                  Address address = 1;
                }
                """);
        assertThatThrownBy(() -> build(
                "order.proto",
                """
                syntax = "proto3";
                import "person.proto";
                message Order {
                  Address destination = 1;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath))))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'order.proto': ")
                .hasMessageContaining("\"Address\"");
    }

    @Test
    public void testMapEntryNamesFollowProtoc()
    {
        FileDescriptor file = build("maps.proto",
                """
                syntax = "proto3";
                message Maps {
                  map<string, int32> simple = 1;
                  map<string, Maps> nested_map = 2;
                  map<int64, string> _leading = 3;
                  map<string, string> a_b_c = 4;
                  map<string, string> already_Entry = 5;
                  message Declared {
                    string unrelated = 1;
                  }
                  Declared declared = 6;
                }
                """);
        Descriptor maps = file.findMessageTypeByName("Maps");
        assertThat(maps.findFieldByName("simple").getMessageType().getName()).isEqualTo("SimpleEntry");
        assertThat(maps.findFieldByName("_leading").getMessageType().getName()).isEqualTo("LeadingEntry");
        assertThat(maps.findFieldByName("a_b_c").getMessageType().getName()).isEqualTo("ABCEntry");
        assertThat(maps.findFieldByName("already_Entry").getMessageType().getName()).isEqualTo("AlreadyEntryEntry");
        FieldDescriptor nestedMap = maps.findFieldByName("nested_map");
        assertThat(nestedMap.isMapField()).isTrue();
        assertThat(nestedMap.getMessageType().findFieldByName("value").getMessageType()).isSameAs(maps);
        assertThat(maps.findFieldByName("declared").getMessageType().getOptions().getMapEntry()).isFalse();
        // declared nested messages come first, in source order, followed by the map entry types
        assertThat(maps.getNestedTypes()).extracting(Descriptor::getName).containsExactly("Declared", "SimpleEntry", "NestedMapEntry", "LeadingEntry", "ABCEntry", "AlreadyEntryEntry");

        // a declared message named like a map entry collides, as it does with protoc
        assertThatThrownBy(() -> build("collision.proto",
                """
                syntax = "proto3";
                message Maps {
                  map<string, string> nested_map = 1;
                  message NestedMapEntry {}
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Invalid protobuf schema 'collision.proto': Maps.NestedMapEntry: \"NestedMapEntry\" is already defined in \"Maps\".");
    }

    @Test
    public void testGroupsAreRejected()
    {
        assertThatThrownBy(() -> build("groups.proto",
                """
                syntax = "proto2";
                message Legacy {
                  optional group Result = 1 {
                    optional string url = 2;
                  }
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Groups are not supported: message 'Legacy' in protobuf schema 'groups.proto'");
    }

    @Test
    public void testDuplicateDeclarationsAreRejected()
    {
        assertThatThrownBy(() -> build("dup.proto",
                """
                syntax = "proto3";
                message Dup {
                  string a = 1;
                  string b = 1;
                }
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid protobuf schema 'dup.proto': ")
                .hasMessageContaining("Field number 1 has already been used");

        assertThatThrownBy(() -> build("dup.proto",
                """
                syntax = "proto3";
                message Dup {}
                message Dup {}
                """))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Failed to parse protobuf schema 'dup.proto': Dup (dup.proto:3:1) is already defined at dup.proto:2:1");
    }

    @Test
    public void testDescriptorsRoundTripMessages()
            throws Exception
    {
        FileDescriptor file = build("rt.proto",
                """
                syntax = "proto3";
                message Pair {
                  string key = 1;
                  oneof value {
                    int64 number = 2;
                    string text = 3;
                  }
                  map<string, int32> counts = 4;
                }
                """);
        Descriptor pair = file.findMessageTypeByName("Pair");
        Descriptor entry = pair.findFieldByName("counts").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(pair)
                .setField(pair.findFieldByName("key"), "k")
                .setField(pair.findFieldByName("text"), "t")
                .addRepeatedField(pair.findFieldByName("counts"), DynamicMessage.newBuilder(entry)
                        .setField(entry.findFieldByName("key"), "a")
                        .setField(entry.findFieldByName("value"), 7)
                        .build())
                .build();
        DynamicMessage parsed = DynamicMessage.parseFrom(pair, message.toByteArray());
        assertThat(parsed).isEqualTo(message);
        assertThat(parsed.getOneofFieldDescriptor(pair.getOneofs().getFirst()).getName()).isEqualTo("text");
    }

    @Test
    public void testMapValueTypesKeepTheirQualification()
    {
        ImmutableMap<String, String> imports = ImmutableMap.of(
                "common/item.proto",
                """
                syntax = "proto3";
                package common;
                message Item {
                  string id = 1;
                }
                """);
        FileDescriptor file = build(
                "maps.proto",
                """
                syntax = "proto3";
                package app;
                import "common/item.proto";
                import "google/protobuf/struct.proto";
                enum Color {
                  COLOR_UNSPECIFIED = 0;
                  COLOR_RED = 1;
                }
                message Outer {
                  message Inner {
                    string id = 1;
                  }
                }
                message Bundle {
                  map<string, common.Item> items = 1;
                  map<string, google.protobuf.Value> metadata = 2;
                  map<string, Outer.Inner> inners = 3;
                  map<string, Color> colors = 4;
                  map<bool, Bundle> bundles = 5;
                  map<fixed64, bytes> blobs = 6;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath)));
        Descriptor bundle = file.findMessageTypeByName("Bundle");
        for (FieldDescriptor field : bundle.getFields()) {
            assertThat(field.isMapField()).as("%s is a map field", field.getName()).isTrue();
            assertThat(field.getMessageType().getOptions().getMapEntry()).isTrue();
            assertThat(field.getMessageType().findFieldByName("key").getNumber()).isEqualTo(1);
            assertThat(field.getMessageType().findFieldByName("value").getNumber()).isEqualTo(2);
        }
        assertThat(mapValue(bundle, "items").getMessageType().getFullName()).isEqualTo("common.Item");
        assertThat(mapValue(bundle, "metadata").getMessageType()).isSameAs(Value.getDescriptor());
        assertThat(mapValue(bundle, "inners").getMessageType().getFullName()).isEqualTo("app.Outer.Inner");
        assertThat(mapValue(bundle, "colors").getEnumType().getFullName()).isEqualTo("app.Color");
        assertThat(mapValue(bundle, "bundles").getMessageType()).isSameAs(bundle);
        assertThat(bundle.findFieldByName("bundles").getMessageType().findFieldByName("key").getType()).isEqualTo(FieldDescriptor.Type.BOOL);
        assertThat(bundle.findFieldByName("blobs").getMessageType().findFieldByName("key").getType()).isEqualTo(FieldDescriptor.Type.FIXED64);
        assertThat(mapValue(bundle, "blobs").getType()).isEqualTo(FieldDescriptor.Type.BYTES);
    }

    @Test
    public void testMapsAreWireCompatibleWithGeneratedCode()
            throws Exception
    {
        // a twin of google.protobuf.Struct must accept the bytes of the real one, and vice versa
        FileDescriptor file = build("mirror.proto",
                """
                syntax = "proto3";
                import "google/protobuf/struct.proto";
                message Mirror {
                  map<string, google.protobuf.Value> fields = 1;
                }
                """);
        Descriptor mirror = file.findMessageTypeByName("Mirror");
        Struct struct = Struct.newBuilder()
                .putFields("k", Value.newBuilder().setStringValue("v").build())
                .putFields("n", Value.newBuilder().setNumberValue(1.5).build())
                .build();

        DynamicMessage parsed = DynamicMessage.parseFrom(mirror, struct.toByteArray());
        FieldDescriptor fields = mirror.findFieldByName("fields");
        assertThat(parsed.getRepeatedFieldCount(fields)).isEqualTo(2);
        DynamicMessage entry = (DynamicMessage) parsed.getRepeatedField(fields, 0);
        assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("key"))).isEqualTo("k");
        assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("value"))).isEqualTo(Value.newBuilder().setStringValue("v").build());
        assertThat(Struct.parseFrom(parsed.toByteArray())).isEqualTo(struct);
    }

    @Test
    public void testPackageLessFiles()
    {
        ImmutableMap<String, String> imports = ImmutableMap.of(
                "bare.proto",
                """
                syntax = "proto3";
                message Bare {
                  string id = 1;
                  message Inner {}
                  enum Kind {
                    NONE = 0;
                  }
                }
                """);
        FileDescriptor file = build(
                "user.proto",
                """
                syntax = "proto3";
                package app;
                import "bare.proto";
                message User {
                  Bare bare = 1;
                  Bare.Inner inner = 2;
                  .Bare.Kind kind = 3;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath)));
        FileDescriptor bare = file.getDependencies().getFirst();
        assertThat(bare.getPackage()).isEmpty();
        assertThat(bare.findMessageTypeByName("Bare").getFullName()).isEqualTo("Bare");
        assertThat(bare.findMessageTypeByName("Bare").findNestedTypeByName("Inner").getFullName()).isEqualTo("Bare.Inner");
        Descriptor user = file.findMessageTypeByName("User");
        assertThat(user.getFullName()).isEqualTo("app.User");
        assertThat(user.findFieldByName("bare").getMessageType().getFullName()).isEqualTo("Bare");
        assertThat(user.findFieldByName("inner").getMessageType().getFullName()).isEqualTo("Bare.Inner");
        assertThat(user.findFieldByName("kind").getEnumType().getFullName()).isEqualTo("Bare.Kind");
    }

    @Test
    public void testBuiltInFilesTakePrecedenceOverResolver()
    {
        // a resolver that offers its own copy of a built-in file is never consulted for it
        List<String> requested = new ArrayList<>();
        FileDescriptor file = build(
                "stamped.proto",
                """
                syntax = "proto3";
                import "google/protobuf/timestamp.proto";
                import "google/type/date.proto";
                import "other.proto";
                message Stamped {
                  google.protobuf.Timestamp at = 1;
                  google.type.Date on = 2;
                  Other other = 3;
                }
                """,
                importPath -> {
                    requested.add(importPath);
                    return switch (importPath) {
                        case "google/protobuf/timestamp.proto", "google/type/date.proto" -> Optional.of("syntax = \"proto3\"; message Wrong {}");
                        case "other.proto" -> Optional.of("syntax = \"proto3\"; message Other {}");
                        default -> Optional.empty();
                    };
                });
        assertThat(requested).containsExactly("other.proto");
        Descriptor stamped = file.findMessageTypeByName("Stamped");
        assertThat(stamped.findFieldByName("at").getMessageType()).isSameAs(Timestamp.getDescriptor());
        assertThat(stamped.findFieldByName("on").getMessageType()).isSameAs(Date.getDescriptor());
        assertThat(file.getDependencies()).extracting(FileDescriptor::getName).containsExactly("google/protobuf/timestamp.proto", "google/type/date.proto", "other.proto");
    }

    @Test
    public void testProto2DefaultValues()
    {
        FileDescriptor file = build("defaults.proto",
                """
                syntax = "proto2";
                message Defaults {
                  optional int32 count = 1 [default = 42];
                  optional sint64 offset = 2 [default = -7];
                  optional double ratio = 3 [default = 0.25];
                  optional float scale = 4 [default = -1.5];
                  optional bool enabled = 5 [default = true];
                  optional string label = 6 [default = "a \\"quoted\\" label"];
                  optional bytes raw = 7 [default = "\\001\\002"];
                  optional uint64 big = 8 [default = 18446744073709551615];
                  optional string empty = 9;
                  optional int32 zero = 10;
                }
                """);
        Descriptor defaults = file.findMessageTypeByName("Defaults");
        assertThat(defaults.findFieldByName("count").getDefaultValue()).isEqualTo(42);
        assertThat(defaults.findFieldByName("offset").getDefaultValue()).isEqualTo(-7L);
        assertThat(defaults.findFieldByName("ratio").getDefaultValue()).isEqualTo(0.25);
        assertThat(defaults.findFieldByName("scale").getDefaultValue()).isEqualTo(-1.5f);
        assertThat(defaults.findFieldByName("enabled").getDefaultValue()).isEqualTo(true);
        assertThat(defaults.findFieldByName("label").getDefaultValue()).isEqualTo("a \"quoted\" label");
        assertThat(defaults.findFieldByName("raw").getDefaultValue()).isEqualTo(ByteString.copyFrom(new byte[] {1, 2}));
        assertThat(defaults.findFieldByName("big").getDefaultValue()).isEqualTo(-1L);
        assertThat(defaults.findFieldByName("empty").getDefaultValue()).isEqualTo("");
        assertThat(defaults.findFieldByName("zero").getDefaultValue()).isEqualTo(0);
        assertThat(defaults.findFieldByName("count").hasDefaultValue()).isTrue();
        assertThat(defaults.findFieldByName("zero").hasDefaultValue()).isFalse();
    }

    @Test
    public void testProto2DefaultValueEscapesAndSpecialValues()
    {
        // what protoc records for these defaults: bytes as their escaped bytes, strings as their text
        FileDescriptor file = build("escapes.proto",
                """
                syntax = "proto2";
                enum Color {
                  RED = 0;
                  BLUE = 1;
                }
                message Escapes {
                  optional bytes high = 1 [default = "\\377\\x80\\000"];
                  optional bytes backslash = 2 [default = "a\\\\b"];
                  optional bytes quote = 3 [default = "\\"'"];
                  optional string text_backslash = 4 [default = "a\\\\b"];
                  optional string unicode = 5 [default = "\u00e9t\u00e9"];
                  optional bytes unicode_bytes = 6 [default = "\u20ac"];
                  optional double positive_infinity = 7 [default = inf];
                  optional float negative_infinity = 8 [default = -inf];
                  optional double not_a_number = 9 [default = nan];
                  optional int32 hex = 10 [default = 0x1F];
                  optional int64 octal = 11 [default = 017];
                  optional int32 min = 12 [default = -2147483648];
                  optional sfixed64 min64 = 13 [default = -9223372036854775808];
                  optional uint32 max32 = 14 [default = 4294967295];
                  optional Color color = 15 [default = BLUE];
                }
                """);
        Descriptor escapes = file.findMessageTypeByName("Escapes");
        assertThat(escapes.findFieldByName("high").getDefaultValue()).isEqualTo(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0x80, 0}));
        assertThat(escapes.findFieldByName("backslash").getDefaultValue()).isEqualTo(ByteString.copyFromUtf8("a\\b"));
        assertThat(escapes.findFieldByName("quote").getDefaultValue()).isEqualTo(ByteString.copyFromUtf8("\"'"));
        assertThat(escapes.findFieldByName("text_backslash").getDefaultValue()).isEqualTo("a\\b");
        assertThat(escapes.findFieldByName("unicode").getDefaultValue()).isEqualTo("\u00e9t\u00e9");
        // protoc encodes other text in a bytes default as UTF-8
        assertThat(escapes.findFieldByName("unicode_bytes").getDefaultValue()).isEqualTo(ByteString.copyFromUtf8("\u20ac"));
        assertThat(escapes.findFieldByName("positive_infinity").getDefaultValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(escapes.findFieldByName("negative_infinity").getDefaultValue()).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat((double) escapes.findFieldByName("not_a_number").getDefaultValue()).isNaN();
        assertThat(escapes.findFieldByName("hex").getDefaultValue()).isEqualTo(31);
        assertThat(escapes.findFieldByName("octal").getDefaultValue()).isEqualTo(15L);
        assertThat(escapes.findFieldByName("min").getDefaultValue()).isEqualTo(Integer.MIN_VALUE);
        assertThat(escapes.findFieldByName("min64").getDefaultValue()).isEqualTo(Long.MIN_VALUE);
        assertThat(escapes.findFieldByName("max32").getDefaultValue()).isEqualTo(-1);
        assertThat(((EnumValueDescriptor) escapes.findFieldByName("color").getDefaultValue()).getName()).isEqualTo("BLUE");
    }

    @Test
    public void testReservedDeclarationsAndEmptyMessages()
            throws Exception
    {
        FileDescriptor file = build("reserved.proto",
                """
                syntax = "proto3";
                message Empty {}
                message Reserved {
                  reserved 2, 15 to 20, 1000 to 18999;
                  reserved "old_name", "older_name";
                  string name = 1;
                  int32 last = 536870911;
                }
                enum Status {
                  reserved 5;
                  reserved "LEGACY";
                  STATUS_UNSPECIFIED = 0;
                }
                """);
        assertThat(file.findMessageTypeByName("Empty").getFields()).isEmpty();
        Descriptor reserved = file.findMessageTypeByName("Reserved");
        assertThat(reserved.getFields()).extracting(FieldDescriptor::getName).containsExactly("name", "last");
        assertThat(reserved.findFieldByName("last").getNumber()).isEqualTo(536870911);
        assertThat(file.findEnumTypeByName("Status").getValues()).hasSize(1);

        DynamicMessage message = DynamicMessage.newBuilder(reserved)
                .setField(reserved.findFieldByName("last"), 5)
                .build();
        assertThat(DynamicMessage.parseFrom(reserved, message.toByteArray())).isEqualTo(message);
    }

    @Test
    public void testReservedNumbersAndNamesAreNotReusable()
    {
        // protoc rejects every one of these
        Map<String, String> declarations = ImmutableMap.<String, String>builder()
                .put("message Reserved { reserved 2, 15 to 20; int32 field = 17; }", "Field 'field' in message 'Reserved' in protobuf schema 'reserved.proto' uses reserved number 17")
                .put("message Reserved { reserved 2; int32 field = 2; }", "Field 'field' in message 'Reserved' in protobuf schema 'reserved.proto' uses reserved number 2")
                .put("message Reserved { reserved 1000 to max; int32 last = 536870911; }", "Field 'last' in message 'Reserved' in protobuf schema 'reserved.proto' uses reserved number 536870911")
                .put("message Reserved { reserved \"old_name\"; string old_name = 1; }", "Field 'old_name' in message 'Reserved' in protobuf schema 'reserved.proto' uses a reserved name")
                .put("message Reserved { reserved 3; oneof choice { int32 number = 3; } }", "Field 'number' in message 'Reserved' in protobuf schema 'reserved.proto' uses reserved number 3")
                .put("message Outer { message Inner { reserved 4 to 6; int64 id = 5; } }", "Field 'id' in message 'Inner' in protobuf schema 'reserved.proto' uses reserved number 5")
                .put("enum Status { reserved 5; STATUS_UNSPECIFIED = 0; FIVE = 5; }", "Value 'FIVE' in enum 'Status' in protobuf schema 'reserved.proto' uses reserved number 5")
                .put("enum Status { reserved \"LEGACY\"; STATUS_UNSPECIFIED = 0; LEGACY = 1; }", "Value 'LEGACY' in enum 'Status' in protobuf schema 'reserved.proto' uses a reserved name")
                .put("message Outer { enum Status { reserved 1 to 3; STATUS_UNSPECIFIED = 0; TWO = 2; } }", "Value 'TWO' in enum 'Status' in protobuf schema 'reserved.proto' uses reserved number 2")
                .put("message Reserved { reserved 7; map<string, string> attributes = 7; }", "Field 'attributes' in message 'Reserved' in protobuf schema 'reserved.proto' uses reserved number 7")
                .put("message Reserved { reserved \"a\", \"b\"; reserved 9; string b = 1; }", "Field 'b' in message 'Reserved' in protobuf schema 'reserved.proto' uses a reserved name")
                .put("enum Status { reserved -5 to -1; STATUS_UNSPECIFIED = 0; NEGATIVE = -3; }", "Value 'NEGATIVE' in enum 'Status' in protobuf schema 'reserved.proto' uses reserved number -3")
                .buildOrThrow();
        declarations.forEach((declaration, message) ->
                assertThatThrownBy(() -> build("reserved.proto", "syntax = \"proto3\";\n" + declaration))
                        .as(declaration)
                        .isInstanceOf(TrinoException.class)
                        .hasMessage(message)
                        .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(INVALID_PROTO_FILE.toErrorCode())));

        // numbers next to a reserved range, and names that differ in case, are free
        FileDescriptor file = build("reserved.proto",
                """
                syntax = "proto3";
                message Reserved {
                  reserved 2, 15 to 20, 1000 to max;
                  reserved "old_name";
                  int32 before = 14;
                  int32 after = 21;
                  int32 last = 999;
                  string Old_Name = 1;
                }
                """);
        assertThat(file.findMessageTypeByName("Reserved").getFields()).extracting(FieldDescriptor::getNumber).containsExactly(14, 21, 999, 1);
    }

    private static FieldDescriptor mapValue(Descriptor message, String field)
    {
        return message.findFieldByName(field).getMessageType().findFieldByName("value");
    }

    private static FileDescriptor build(String fileName, String source)
    {
        return build(fileName, source, NO_IMPORTS);
    }

    private static FileDescriptor build(String fileName, String source, ProtoFileDescriptorBuilder.ImportResolver importResolver)
    {
        return ProtoFileDescriptorBuilder.buildFileDescriptor(fileName, source, importResolver);
    }
}
