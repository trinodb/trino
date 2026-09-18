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
package io.trino.plugin.deltalake.delete;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Base85 using the 4 byte block aligned encoding and character set from <a href="https://rfc.zeromq.org/spec/32/">Z85</a>,
 * as used by the <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-format">Delta Lake deletion vector format</a>.
 * <p>
 * Derived from {@code io.delta.kernel.internal.deletionvectors.Base85Codec} (Apache License 2.0).
 */
final class Base85Codec
{
    private static final long BASE = 85L;
    private static final long BASE_2ND_POWER = BASE * BASE;
    private static final long BASE_3RD_POWER = BASE * BASE * BASE;
    private static final long BASE_4TH_POWER = BASE * BASE * BASE * BASE;

    // UUIDs always encode into 20 characters
    public static final int ENCODED_UUID_LENGTH = 20;

    private static final byte[] ENCODE_MAP = buildEncodeMap();
    private static final byte[] DECODE_MAP = buildDecodeMap();

    private Base85Codec() {}

    private static byte[] buildEncodeMap()
    {
        byte[] map = new byte[85];
        int i = 0;
        for (char c = '0'; c <= '9'; c++) {
            map[i] = (byte) c;
            i++;
        }
        for (char c = 'a'; c <= 'z'; c++) {
            map[i] = (byte) c;
            i++;
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            map[i] = (byte) c;
            i++;
        }
        for (char c : ".-:+=^!/*?&<>()[]{}@%$#".toCharArray()) {
            map[i] = (byte) c;
            i++;
        }
        return map;
    }

    private static byte[] buildDecodeMap()
    {
        byte[] map = new byte[128];
        Arrays.fill(map, (byte) -1);
        for (int i = 0; i < ENCODE_MAP.length; i++) {
            map[ENCODE_MAP[i]] = (byte) i;
        }
        return map;
    }

    public static String encodeUUID(UUID id)
    {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        buffer.rewind();
        return encodeBlocks(buffer);
    }

    public static UUID decodeUUID(String encoded)
    {
        ByteBuffer buffer = decodeBlocks(encoded);
        checkArgument(buffer.remaining() == 16, "Encoded UUID must decode to exactly 16 bytes: %s", encoded);
        long highBits = buffer.getLong();
        long lowBits = buffer.getLong();
        return new UUID(highBits, lowBits);
    }

    // Encode a byte buffer, which must be 4 byte aligned. Every 4 byte block encodes into 5 characters.
    private static String encodeBlocks(ByteBuffer buffer)
    {
        checkArgument(buffer.remaining() % 4 == 0, "Input must be 4 byte aligned");
        byte[] output = new byte[buffer.remaining() / 4 * 5];
        int outputIndex = 0;
        while (buffer.hasRemaining()) {
            long sum = buffer.getInt() & 0xFFFFFFFFL;
            output[outputIndex] = ENCODE_MAP[(int) (sum / BASE_4TH_POWER)];
            sum %= BASE_4TH_POWER;
            output[outputIndex + 1] = ENCODE_MAP[(int) (sum / BASE_3RD_POWER)];
            sum %= BASE_3RD_POWER;
            output[outputIndex + 2] = ENCODE_MAP[(int) (sum / BASE_2ND_POWER)];
            sum %= BASE_2ND_POWER;
            output[outputIndex + 3] = ENCODE_MAP[(int) (sum / BASE)];
            output[outputIndex + 4] = ENCODE_MAP[(int) (sum % BASE)];
            outputIndex += 5;
        }
        return new String(output, US_ASCII);
    }

    // Decode an encoded string, which must be 5 character aligned. Every 5 character block decodes into 4 bytes.
    private static ByteBuffer decodeBlocks(String encoded)
    {
        checkArgument(encoded.length() % 5 == 0, "Input must be 5 character aligned: %s", encoded);
        ByteBuffer buffer = ByteBuffer.allocate(encoded.length() / 5 * 4);
        int inputIndex = 0;
        while (buffer.hasRemaining()) {
            long sum = decodeChar(encoded, inputIndex) * BASE_4TH_POWER;
            sum += decodeChar(encoded, inputIndex + 1) * BASE_3RD_POWER;
            sum += decodeChar(encoded, inputIndex + 2) * BASE_2ND_POWER;
            sum += decodeChar(encoded, inputIndex + 3) * BASE;
            sum += decodeChar(encoded, inputIndex + 4);
            buffer.putInt((int) sum);
            inputIndex += 5;
        }
        buffer.rewind();
        return buffer;
    }

    private static long decodeChar(String encoded, int index)
    {
        char character = encoded.charAt(index);
        checkArgument(character < DECODE_MAP.length && DECODE_MAP[character] != -1, "Input is not valid Z85: %s", encoded);
        return DECODE_MAP[character];
    }
}
