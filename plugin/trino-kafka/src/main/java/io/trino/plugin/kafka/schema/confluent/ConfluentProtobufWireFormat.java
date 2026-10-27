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
import io.trino.spi.TrinoException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The framing the Confluent protobuf serializers put around a message:
 * <pre>
 *   [magic byte 0][4-byte big-endian schema id][message indexes][protobuf message]
 * </pre>
 * The message indexes locate the message type within the schema's file: the index of the
 * top-level message, then one index per nesting step, each counted among the messages declared
 * at that level of the schema text. Enums do not count, and neither do the entry messages that
 * {@code map<K, V>} fields imply, but entry messages that the text declares explicitly with
 * {@code option map_entry = true}, as schema text generated from a descriptor does, count like
 * any other message. This is how the Confluent deserializer resolves the indexes. The indexes
 * are written as a zigzag-encoded varint count followed by zigzag-encoded varint indexes,
 * except that the most common path, {@code [0]}, is written as a single zero byte.
 * <p>
 * The descriptors built from schema text place the implied entry messages after all declared
 * nested messages, so counting every nested message of a descriptor yields these indexes.
 */
public final class ConfluentProtobufWireFormat
{
    public static final byte MAGIC_BYTE = 0;

    private static final int HEADER_LENGTH = 5;
    // nesting deeper than this is not a message index path but a misread record
    private static final int MAX_MESSAGE_INDEXES = 128;

    private ConfluentProtobufWireFormat() {}

    public record Frame(int schemaId, List<Integer> messageIndexes, int payloadOffset)
    {
        public Frame
        {
            messageIndexes = ImmutableList.copyOf(requireNonNull(messageIndexes, "messageIndexes is null"));
        }
    }

    public static Frame readFrame(byte[] data)
    {
        requireNonNull(data, "data is null");
        if (data.length < HEADER_LENGTH) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Record is too short to be in the Confluent wire format");
        }
        if (data[0] != MAGIC_BYTE) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Record does not start with the Confluent wire format magic byte");
        }
        int schemaId = ((data[1] & 0xFF) << 24) | ((data[2] & 0xFF) << 16) | ((data[3] & 0xFF) << 8) | (data[4] & 0xFF);
        VarintReader reader = new VarintReader(data, HEADER_LENGTH);
        int count = reader.read();
        if (count == 0) {
            return new Frame(schemaId, ImmutableList.of(0), reader.position());
        }
        if (count < 0 || count > MAX_MESSAGE_INDEXES) {
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, format("Record has an invalid message index count: %s", count));
        }
        ImmutableList.Builder<Integer> indexes = ImmutableList.builderWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            int index = reader.read();
            if (index < 0) {
                throw new TrinoException(INVALID_PROTOBUF_MESSAGE, format("Record has a negative message index: %s", index));
            }
            indexes.add(index);
        }
        return new Frame(schemaId, indexes.build(), reader.position());
    }

    public static byte[] frame(int schemaId, List<Integer> messageIndexes, byte[] payload)
    {
        requireNonNull(messageIndexes, "messageIndexes is null");
        requireNonNull(payload, "payload is null");
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_LENGTH + 2 * messageIndexes.size() + 1 + payload.length);
        out.write(MAGIC_BYTE);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        if (messageIndexes.equals(ImmutableList.of(0))) {
            out.write(0);
        }
        else {
            writeVarint(out, messageIndexes.size());
            for (int index : messageIndexes) {
                writeVarint(out, index);
            }
        }
        out.writeBytes(payload);
        return out.toByteArray();
    }

    /**
     * Returns the message the index path points at within the file, if the path exists and does not point at a map entry.
     */
    public static Optional<Descriptor> findMessage(FileDescriptor file, List<Integer> messageIndexes)
    {
        if (messageIndexes.isEmpty()) {
            return Optional.empty();
        }
        List<Descriptor> candidates = file.getMessageTypes();
        Descriptor message = null;
        for (int index : messageIndexes) {
            if (index < 0 || index >= candidates.size()) {
                return Optional.empty();
            }
            message = candidates.get(index);
            candidates = message.getNestedTypes();
        }
        if (message.getOptions().getMapEntry()) {
            return Optional.empty();
        }
        return Optional.of(message);
    }

    /**
     * Returns the index path of a message within its file, as written to the frame instead of the message name.
     */
    public static List<Integer> messageIndexes(Descriptor message)
    {
        checkArgument(!message.getOptions().getMapEntry(), "Message %s is the entry type of a map field", message.getFullName());
        Deque<Integer> indexes = new ArrayDeque<>();
        Descriptor current = message;
        while (current.getContainingType() != null) {
            indexes.addFirst(current.getIndex());
            current = current.getContainingType();
        }
        indexes.addFirst(current.getIndex());
        return ImmutableList.copyOf(indexes);
    }

    private static void writeVarint(ByteArrayOutputStream out, int value)
    {
        int encoded = (value << 1) ^ (value >> 31);
        while ((encoded & 0xFFFFFF80) != 0) {
            out.write((encoded & 0x7F) | 0x80);
            encoded >>>= 7;
        }
        out.write(encoded & 0x7F);
    }

    private static final class VarintReader
    {
        private final byte[] data;
        private int position;

        private VarintReader(byte[] data, int position)
        {
            this.data = data;
            this.position = position;
        }

        public int position()
        {
            return position;
        }

        public int read()
        {
            int value = 0;
            for (int shift = 0; shift < 32; shift += 7) {
                if (position >= data.length) {
                    throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Record ends in the middle of the Confluent wire format message indexes");
                }
                byte b = data[position++];
                value |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return (value >>> 1) ^ -(value & 1);
                }
            }
            throw new TrinoException(INVALID_PROTOBUF_MESSAGE, "Record has a malformed varint in the Confluent wire format message indexes");
        }
    }
}
