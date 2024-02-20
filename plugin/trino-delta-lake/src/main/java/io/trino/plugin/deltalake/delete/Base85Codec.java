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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import com.google.common.primitives.SignedBytes;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

// This implements Base85 using the 4 byte block aligned encoding and character set from Z85 https://rfc.zeromq.org/spec/32
// Delta Lake implementation is https://github.com/delta-io/delta/blob/master/kernel/kernel-api/src/main/java/io/delta/kernel/internal/deletionvectors/Base85Codec.java
public final class Base85Codec
{
    @VisibleForTesting
    static final int BASE = 85;
    @VisibleForTesting
    static final int BASE_2ND_POWER = IntMath.pow(BASE, 2);
    @VisibleForTesting
    static final int BASE_3RD_POWER = IntMath.pow(BASE, 3);
    @VisibleForTesting
    static final int BASE_4TH_POWER = IntMath.pow(BASE, 4);

    private static final int ASCII_BITMASK = 0x7F;

    // UUIDs always encode into 20 characters
    static final int ENCODED_UUID_LENGTH = 20;

    private static final String BASE85_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

    @VisibleForTesting
    static final byte[] ENCODE_MAP = BASE85_CHARACTERS.getBytes(UTF_8);

    // The bitmask is the same as largest possible value, so the length of the array must be one greater.
    static final byte[] DECODE_MAP = new byte[ASCII_BITMASK + 1];

    static {
        // Following loop doesn't fill all values
        Arrays.fill(DECODE_MAP, (byte) -1);
        for (int i = 0; i < ENCODE_MAP.length; i++) {
            DECODE_MAP[ENCODE_MAP[i]] = SignedBytes.checkedCast(i);
        }
    }

    private Base85Codec() {}

    public static byte[] decode(String encoded)
    {
        checkArgument(encoded.length() % 5 == 0, "Input should be 5 character aligned");
        byte[] buffer = new byte[encoded.length() / 5 * 4];

        int outputIndex = 0;
        for (int inputIndex = 0; inputIndex < encoded.length(); inputIndex += 5) {
            // word is a container for 32-bit of decoded output. Arithmetics below treat it as unsigned 32-bit integer (consciously overflow)
            int word = 0;
            word += decodeInputChar(encoded.charAt(inputIndex)) * BASE_4TH_POWER;
            word += decodeInputChar(encoded.charAt(inputIndex + 1)) * BASE_3RD_POWER;
            word += decodeInputChar(encoded.charAt(inputIndex + 2)) * BASE_2ND_POWER;
            word += decodeInputChar(encoded.charAt(inputIndex + 3)) * BASE;
            word += decodeInputChar(encoded.charAt(inputIndex + 4));

            buffer[outputIndex] = (byte) (word >> 24);
            //noinspection NumericCastThatLosesPrecision
            buffer[outputIndex + 1] = (byte) (word >> 16);
            //noinspection NumericCastThatLosesPrecision
            buffer[outputIndex + 2] = (byte) (word >> 8);
            //noinspection NumericCastThatLosesPrecision
            buffer[outputIndex + 3] = (byte) word;
            outputIndex += 4;
        }
        checkState(outputIndex == buffer.length);
        return buffer;
    }

    private static int decodeInputChar(char input)
    {
        checkArgument(input == (input & ASCII_BITMASK), "Input character is not ASCII: [%s]", input);
        byte decoded = DECODE_MAP[input & ASCII_BITMASK];
        checkArgument(decoded != (byte) -1, "Invalid input character: [%s]", input);
        return decoded;
    }
}
