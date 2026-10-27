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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.Frame;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.findMessage;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.frame;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.messageIndexes;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.readFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConfluentProtobufWireFormat
{
    private static final byte[] PAYLOAD = {0x08, 0x01};

    @Test
    public void testReadFrameOfFirstMessage()
    {
        Frame frame = readFrame(new byte[] {0, 0, 0, 0, 42, 0, 0x08, 0x01});
        assertThat(frame.schemaId()).isEqualTo(42);
        assertThat(frame.messageIndexes()).containsExactly(0);
        assertThat(frame.payloadOffset()).isEqualTo(6);
    }

    @Test
    public void testReadFrameOfNestedMessage()
    {
        // two zigzag-encoded indexes, 1 and 3
        Frame frame = readFrame(new byte[] {0, 0, 0, 1, 0, 4, 2, 6, 0x08, 0x01});
        assertThat(frame.schemaId()).isEqualTo(256);
        assertThat(frame.messageIndexes()).containsExactly(1, 3);
        assertThat(frame.payloadOffset()).isEqualTo(8);
    }

    @Test
    public void testReadFrameWithMultiByteIndex()
    {
        // index 300 is zigzag-encoded as 600, which takes two varint bytes
        Frame frame = readFrame(new byte[] {0, 0, 0, 0, 1, 2, (byte) 0xD8, 0x04});
        assertThat(frame.messageIndexes()).containsExactly(300);
        assertThat(frame.payloadOffset()).isEqualTo(8);
    }

    @Test
    public void testReadFrameWithLargestSchemaId()
    {
        Frame frame = readFrame(new byte[] {0, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0});
        assertThat(frame.schemaId()).isEqualTo(Integer.MAX_VALUE);
        assertThat(frame.payloadOffset()).isEqualTo(6);
    }

    @Test
    public void testReadFrameWithEmptyPayload()
    {
        Frame frame = readFrame(new byte[] {0, 0, 0, 0, 7, 0});
        assertThat(frame.schemaId()).isEqualTo(7);
        assertThat(frame.payloadOffset()).isEqualTo(6);
    }

    @Test
    public void testReadFrameRejectsShortRecord()
    {
        assertInvalid(new byte[0], "Record is too short to be in the Confluent wire format");
        assertInvalid(new byte[] {0, 0, 0, 0}, "Record is too short to be in the Confluent wire format");
    }

    @Test
    public void testReadFrameRejectsWrongMagicByte()
    {
        assertInvalid(new byte[] {1, 0, 0, 0, 1, 0}, "Record does not start with the Confluent wire format magic byte");
    }

    @Test
    public void testReadFrameRejectsTruncatedIndexes()
    {
        // header only, no message indexes at all
        assertInvalid(new byte[] {0, 0, 0, 0, 1}, "Record ends in the middle of the Confluent wire format message indexes");
        // count of two, but only one index
        assertInvalid(new byte[] {0, 0, 0, 0, 1, 4, 2}, "Record ends in the middle of the Confluent wire format message indexes");
        // continuation bit set on the last byte
        assertInvalid(new byte[] {0, 0, 0, 0, 1, (byte) 0x80}, "Record ends in the middle of the Confluent wire format message indexes");
    }

    @Test
    public void testReadFrameRejectsNegativeCount()
    {
        // zigzag 1 decodes to -1
        assertInvalid(new byte[] {0, 0, 0, 0, 1, 1}, "Record has an invalid message index count: -1");
    }

    @Test
    public void testReadFrameRejectsExcessiveCount()
    {
        // 129 indexes is zigzag-encoded as 258
        assertInvalid(new byte[] {0, 0, 0, 0, 1, (byte) 0x82, 0x02}, "Record has an invalid message index count: 129");
    }

    @Test
    public void testReadFrameRejectsNegativeIndex()
    {
        assertInvalid(new byte[] {0, 0, 0, 0, 1, 2, 1}, "Record has a negative message index: -1");
    }

    @Test
    public void testReadFrameRejectsMalformedVarint()
    {
        byte[] data = new byte[11];
        Arrays.fill(data, 5, 11, (byte) 0x80);
        assertInvalid(data, "Record has a malformed varint in the Confluent wire format message indexes");
    }

    @Test
    public void testFrameWritesFirstMessageAsSingleZeroByte()
    {
        assertThat(frame(1, ImmutableList.of(0), PAYLOAD)).containsExactly(0, 0, 0, 0, 1, 0, 0x08, 0x01);
    }

    @Test
    public void testFrameWritesIndexPath()
    {
        assertThat(frame(0x01020304, ImmutableList.of(1), PAYLOAD)).containsExactly(0, 1, 2, 3, 4, 2, 2, 0x08, 0x01);
        assertThat(frame(5, ImmutableList.of(0, 0), PAYLOAD)).containsExactly(0, 0, 0, 0, 5, 4, 0, 0, 0x08, 0x01);
    }

    @Test
    public void testFrameRoundTrip()
    {
        List<List<Integer>> paths = ImmutableList.of(
                ImmutableList.of(0),
                ImmutableList.of(1),
                ImmutableList.of(0, 0),
                ImmutableList.of(2, 5, 300),
                ImmutableList.of(Integer.MAX_VALUE));
        for (int schemaId : new int[] {0, 1, 255, 256, 65536, Integer.MAX_VALUE}) {
            for (List<Integer> path : paths) {
                byte[] data = frame(schemaId, path, PAYLOAD);
                Frame frame = readFrame(data);
                assertThat(frame.schemaId()).isEqualTo(schemaId);
                assertThat(frame.messageIndexes()).isEqualTo(path);
                assertThat(Arrays.copyOfRange(data, frame.payloadOffset(), data.length)).isEqualTo(PAYLOAD);
            }
        }
    }

    @Test
    public void testMessageIndexesMatchConfluentEncoding()
    {
        List<List<Integer>> paths = ImmutableList.of(
                ImmutableList.of(0),
                ImmutableList.of(1),
                ImmutableList.of(0, 0),
                ImmutableList.of(3, 1, 2),
                ImmutableList.of(300, 70000));
        for (List<Integer> path : paths) {
            byte[] data = frame(0, path, new byte[0]);
            byte[] indexes = Arrays.copyOfRange(data, 5, data.length);
            assertThat(indexes).isEqualTo(new MessageIndexes(path).toByteArray());
            assertThat(MessageIndexes.readFrom(ByteBuffer.wrap(indexes)).indexes()).isEqualTo(path);
            assertThat(readFrame(data).messageIndexes()).isEqualTo(path);
        }
    }

    @Test
    public void testFindMessageAndMessageIndexes()
            throws Exception
    {
        FileDescriptor file = getFileDescriptor(
                """
                syntax = "proto3";
                message First {
                    map<string, string> attributes = 1;
                    message Inner {
                        message Innermost {}
                    }
                    enum Color { RED = 0; }
                    message Other {}
                }
                enum Kind { PLAIN = 0; }
                message Second {
                    map<string, First> firsts = 1;
                    message Child {}
                }
                """);
        Descriptor first = file.findMessageTypeByName("First");
        Descriptor inner = first.findNestedTypeByName("Inner");
        Descriptor innermost = inner.findNestedTypeByName("Innermost");
        Descriptor other = first.findNestedTypeByName("Other");
        Descriptor second = file.findMessageTypeByName("Second");
        Descriptor child = second.findNestedTypeByName("Child");

        // enums and the entry messages implied by map fields do not count towards the indexes
        assertThat(messageIndexes(first)).containsExactly(0);
        assertThat(messageIndexes(inner)).containsExactly(0, 0);
        assertThat(messageIndexes(innermost)).containsExactly(0, 0, 0);
        assertThat(messageIndexes(other)).containsExactly(0, 1);
        assertThat(messageIndexes(second)).containsExactly(1);
        assertThat(messageIndexes(child)).containsExactly(1, 0);

        for (Descriptor message : ImmutableList.of(first, inner, innermost, other, second, child)) {
            assertThat(findMessage(file, messageIndexes(message))).contains(message);
        }

        assertThat(findMessage(file, ImmutableList.of())).isEmpty();
        assertThat(findMessage(file, ImmutableList.of(2))).isEmpty();
        assertThat(findMessage(file, ImmutableList.of(0, 2))).isEmpty();
        assertThat(findMessage(file, ImmutableList.of(0, 0, 0, 0))).isEmpty();
        assertThat(findMessage(file, ImmutableList.of(1, 1))).isEmpty();
        assertThat(findMessage(file, ImmutableList.of(-1))).isEmpty();
    }

    @Test
    public void testExplicitMapEntryMessagesCountTowardsIndexes()
    {
        // the schema text a Confluent serializer registers for a protoc generated class declares
        // map entry messages explicitly, in the position protoc gives them
        String schema =
                """
                syntax = "proto3";

                message Holder {
                  repeated .Holder.AttributesEntry attributes = 1;
                  .Holder.Inner inner = 2;

                  message AttributesEntry {
                    option map_entry = true;

                    string key = 1;
                    string value = 2;
                  }
                  message Inner {
                    string value = 1;
                  }
                }
                """;
        FileDescriptor file = getFileDescriptor(schema);
        Descriptor holder = file.findMessageTypeByName("Holder");
        Descriptor inner = holder.findNestedTypeByName("Inner");
        assertThat(holder.findFieldByName("attributes").isMapField()).isTrue();

        ProtobufSchema confluentSchema = new ProtobufSchema(schema);
        assertThat(messageIndexes(inner))
                .containsExactly(0, 1)
                .isEqualTo(confluentSchema.toMessageIndexes("Holder.Inner").indexes());
        assertThat(confluentSchema.toMessageName(new MessageIndexes(ImmutableList.of(0, 1)))).isEqualTo("Holder.Inner");
        assertThat(findMessage(file, ImmutableList.of(0, 1))).contains(inner);
        // a record is never of a map entry type
        assertThat(findMessage(file, ImmutableList.of(0, 0))).isEmpty();
    }

    @Test
    public void testMessageIndexesOfMapEntryIsNotSupported()
            throws Exception
    {
        FileDescriptor file = getFileDescriptor(
                """
                syntax = "proto3";
                message Holder {
                    map<string, string> attributes = 1;
                }
                """);
        Descriptor entry = file.findMessageTypeByName("Holder").findNestedTypeByName("AttributesEntry");
        assertThatThrownBy(() -> messageIndexes(entry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Message Holder.AttributesEntry is the entry type of a map field");
    }

    private static void assertInvalid(byte[] data, String message)
    {
        assertThatThrownBy(() -> readFrame(data))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_PROTOBUF_MESSAGE.toErrorCode()))
                .hasMessage(message);
    }
}
