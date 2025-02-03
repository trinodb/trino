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
package io.trino.spi.type;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlVarbinary
{
    @Test
    public void testToHexString()
    {
        for (int lines = 0; lines < 5; lines++) {
            for (int lastLineBytes = 0; lastLineBytes < 32; lastLineBytes++) {
                byte[] bytes = createBytes(lines, lastLineBytes);
                String expectedHex = simpleToHex(bytes);
                assertThat(expectedHex).isEqualTo(new SqlVarbinary(bytes).toHexString());
                assertThat(Base64.getEncoder().encodeToString(bytes)).isEqualTo(new SqlVarbinary(bytes).toString());
            }
        }
    }

    private static String simpleToHex(byte[] bytes)
    {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < bytes.length; ++i) {
            if (i != 0) {
                if (i % 32 == 0) {
                    builder.append("\n");
                }
                else if (i % 8 == 0) {
                    builder.append("   ");
                }
                else {
                    builder.append(" ");
                }
            }

            builder.append(format("%02x", bytes[i] & 0xff));
        }
        return builder.toString();
    }

    private static byte[] createBytes(int lines, int lastLineLength)
    {
        byte[] bytes = new byte[(lines * 32) + lastLineLength];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
