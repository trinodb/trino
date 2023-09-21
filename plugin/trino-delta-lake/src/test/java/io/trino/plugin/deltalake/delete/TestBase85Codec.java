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

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.delete.Base85Codec.BASE;
import static io.trino.plugin.deltalake.delete.Base85Codec.BASE_2ND_POWER;
import static io.trino.plugin.deltalake.delete.Base85Codec.BASE_3RD_POWER;
import static io.trino.plugin.deltalake.delete.Base85Codec.BASE_4TH_POWER;
import static io.trino.plugin.deltalake.delete.Base85Codec.ENCODE_MAP;
import static io.trino.plugin.deltalake.delete.Base85Codec.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBase85Codec
{
    @Test
    public void testDecodeBlocksIllegalCharacter()
    {
        assertThatThrownBy(() -> decode("ab" + 0x7F + "de")).hasMessageContaining("Input should be 5 character aligned");

        assertThatThrownBy(() -> decode("abîde")).hasMessageContaining("Input character is not ASCII: [î]");
        assertThatThrownBy(() -> decode("abπde")).hasMessageContaining("Input character is not ASCII: [π]");
        assertThatThrownBy(() -> decode("ab\"de")).hasMessageContaining("Invalid input character: [\"]");
    }

    @Test
    public void testEncodeBytes()
    {
        // The test case comes from https://rfc.zeromq.org/spec/32
        //noinspection NumericCastThatLosesPrecision
        byte[] inputBytes = new byte[] {(byte) 0x86, 0x4F, (byte) 0xD2, 0x6F, (byte) 0xB5, 0x59, (byte) 0xF7, 0x5B};
        String encoded = encodeBytes(inputBytes);
        assertThat(encoded).isEqualTo("HelloWorld");
    }

    @Test
    public void testCodecRoundTrip()
    {
        assertThat(encodeBytes(Base85Codec.decode("HelloWorld")))
                .isEqualTo("HelloWorld");
        assertThat(encodeBytes(Base85Codec.decode("wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L")))
                .isEqualTo("wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L");
    }

    @Test
    public void testDecodeBytes()
    {
        String data = "HelloWorld";
        byte[] bytes = Base85Codec.decode(data);
        //noinspection NumericCastThatLosesPrecision
        assertThat(bytes).isEqualTo(new byte[] {(byte) 0x86, 0x4F, (byte) 0xD2, 0x6F, (byte) 0xB5, 0x59, (byte) 0xF7, 0x5B});
    }

    private static String encodeBytes(byte[] input)
    {
        if (input.length % 4 == 0) {
            return encodeBlocks(ByteBuffer.wrap(input));
        }
        int alignedLength = ((input.length + 4) / 4) * 4;
        ByteBuffer buffer = ByteBuffer.allocate(alignedLength);
        buffer.put(input);
        while (buffer.hasRemaining()) {
            buffer.put((byte) 0);
        }
        buffer.rewind();
        return encodeBlocks(buffer);
    }

    private static String encodeBlocks(ByteBuffer buffer)
    {
        checkArgument(buffer.remaining() % 4 == 0);
        int numBlocks = buffer.remaining() / 4;
        // Every 4 byte block gets encoded into 5 bytes/chars
        int outputLength = numBlocks * 5;
        byte[] output = new byte[outputLength];
        int outputIndex = 0;

        while (buffer.hasRemaining()) {
            long word = Integer.toUnsignedLong(buffer.getInt()) & 0x00000000ffffffffL;
            output[outputIndex] = ENCODE_MAP[(int) (word / BASE_4TH_POWER)];
            word %= BASE_4TH_POWER;
            output[outputIndex + 1] = ENCODE_MAP[(int) (word / BASE_3RD_POWER)];
            word %= BASE_3RD_POWER;
            output[outputIndex + 2] = ENCODE_MAP[(int) (word / BASE_2ND_POWER)];
            word %= BASE_2ND_POWER;
            output[outputIndex + 3] = ENCODE_MAP[(int) (word / BASE)];
            output[outputIndex + 4] = ENCODE_MAP[(int) (word % BASE)];
            outputIndex += 5;
        }
        return new String(output, UTF_8);
    }
}
