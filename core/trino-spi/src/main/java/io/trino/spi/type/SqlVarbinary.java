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

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.HexFormat;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class SqlVarbinary
        implements Comparable<SqlVarbinary>
{
    private static final HexFormat HEX_FORMAT = HexFormat.of().withDelimiter(" ");
    private static final String WORD_SEPARATOR = "   ";
    private static final int OUTPUT_CHARS_PER_FULL_WORD = (8 * 2) + 7; // two output hex chars per byte, 7 padding chars between them

    private final byte[] bytes;

    public SqlVarbinary(byte[] bytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    @Override
    public int compareTo(SqlVarbinary obj)
    {
        return Arrays.compare(bytes, obj.bytes);
    }

    @JsonValue
    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlVarbinary other = (SqlVarbinary) obj;
        return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public String toString()
    {
        if (bytes.length == 0) {
            return "";
        }
        int fullLineCount = bytes.length / 32;
        int lastLineBytes = bytes.length % 32;

        // 4 full words with 3 word separators and one line break per full line of output
        long totalSize = (long) fullLineCount * ((4 * OUTPUT_CHARS_PER_FULL_WORD) + (3 * WORD_SEPARATOR.length()) + 1);
        if (lastLineBytes == 0) {
            totalSize--; // no final line separator
        }
        else {
            int lastLineWords = lastLineBytes / 8;
            totalSize += (long) lastLineWords * (OUTPUT_CHARS_PER_FULL_WORD + WORD_SEPARATOR.length());
            // whole words and separators on last line
            if (lastLineWords * 8 == lastLineBytes) {
                totalSize -= WORD_SEPARATOR.length(); // last line ends on a word boundary, no separator
            }
            else {
                // Trailing partial word on the last line
                int lastWordBytes = lastLineBytes % 8;
                // 2 hex chars per byte in the last word, plus 1 byte separator between each hex pair
                totalSize += (2L * lastWordBytes) + (lastWordBytes - 1);
            }
        }

        StringBuilder builder = new StringBuilder(toIntExact(totalSize));

        int index = 0;
        for (int i = 0; i < fullLineCount; i++) {
            if (i != 0) {
                builder.append("\n");
            }
            HEX_FORMAT.formatHex(builder, bytes, index, index + 8);
            builder.append(WORD_SEPARATOR);
            index += 8;
            HEX_FORMAT.formatHex(builder, bytes, index, index + 8);
            builder.append(WORD_SEPARATOR);
            index += 8;
            HEX_FORMAT.formatHex(builder, bytes, index, index + 8);
            builder.append(WORD_SEPARATOR);
            index += 8;
            HEX_FORMAT.formatHex(builder, bytes, index, index + 8);
            index += 8;
        }
        if (lastLineBytes > 0) {
            if (fullLineCount > 0) {
                builder.append("\n");
            }
            boolean firstWord = true;
            while (index < bytes.length) {
                if (!firstWord) {
                    builder.append(WORD_SEPARATOR);
                }
                firstWord = false;
                int length = min(8, bytes.length - index);
                HEX_FORMAT.formatHex(builder, bytes, index, index + length);
                index += length;
            }
        }
        return builder.toString();
    }
}
