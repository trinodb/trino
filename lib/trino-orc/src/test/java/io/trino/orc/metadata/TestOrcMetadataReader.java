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
package io.trino.orc.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;
import io.trino.orc.metadata.statistics.StringStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.orc.metadata.OrcMetadataReader.findStringStatisticTruncationPositionForOriginalOrcWriter;
import static io.trino.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static io.trino.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static io.trino.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static io.trino.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcMetadataReader
{
    @Test
    public void testGetMinSlice()
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice minSlice = utf8Slice("");

        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = codePointToUtf8(codePoint);
            if (findStringStatisticTruncationPositionForOriginalOrcWriter(value) == value.length()) {
                assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(value);
            }
            else {
                assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(minSlice);
            }
        }

        // Test with prefix
        Slice prefix = utf8Slice("apple");
        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = concatSlice(prefix, codePointToUtf8(codePoint));
            if (findStringStatisticTruncationPositionForOriginalOrcWriter(value) == value.length()) {
                assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(value);
            }
            else {
                assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(prefix);
            }
        }
    }

    @Test
    public void testGetMaxSlice()
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice maxByte = wrappedBuffer((byte) 0xFF);

        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = codePointToUtf8(codePoint);
            if (findStringStatisticTruncationPositionForOriginalOrcWriter(value) == value.length()) {
                assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(value);
            }
            else {
                assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(maxByte);
            }
        }

        // Test with prefix
        Slice prefix = utf8Slice("apple");
        Slice maxSlice = concatSlice(prefix, maxByte);
        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = concatSlice(prefix, codePointToUtf8(codePoint));
            if (findStringStatisticTruncationPositionForOriginalOrcWriter(value) == value.length()) {
                assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(value);
            }
            else {
                assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(maxSlice);
            }
        }
    }

    @Test
    void testToStripeInformationLargeRowCount()
            throws IOException
    {
        OrcProto.StripeInformation stripe = OrcProto.StripeInformation.newBuilder()
                .setNumberOfRows(2 * (long) Integer.MAX_VALUE)
                .setOffset(0)
                .setIndexLength(100)
                .setDataLength(200)
                .setFooterLength(50)
                .build();

        OrcProto.Type.Builder typeBigInt = OrcProto.Type.newBuilder()
                .setKind(OrcProto.Type.Kind.LONG);

        OrcProto.Footer footer = OrcProto.Footer.newBuilder()
                .addTypes(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT))
                .addTypes(typeBigInt)
                .addTypes(typeBigInt)
                .addAllStripes(List.of(stripe))
                .build();
        InputStream input = new ByteArrayInputStream(footer.toByteArray());

        OrcMetadataReader orcMetadataReader = new OrcMetadataReader(new OrcReaderOptions());
        Footer actualFooter = orcMetadataReader.readFooter(ORIGINAL, input);
        assertThat(actualFooter.getStripes().getFirst().getNumberOfRows()).isEqualTo(stripe.getNumberOfRows());
    }

    private static final Slice STRING_APPLE = utf8Slice("apple");
    private static final Slice STRING_OESTERREICH = utf8Slice("\u00D6sterreich");
    private static final Slice STRING_DULIOE_DULIOE = utf8Slice("Duli\u00F6 duli\u00F6");
    private static final Slice STRING_FAITH_HOPE_LOVE = utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B");
    private static final Slice STRING_NAIVE = utf8Slice("na\u00EFve");
    private static final Slice STRING_OO = utf8Slice("\uD801\uDC2Dend");

    // length increase when cast to lower case, and ends with invalid character
    private static final Slice INVALID_SEQUENCE_TO_LOWER_EXPANDS = wrappedBuffer((byte) 0xC8, (byte) 0xBA, (byte) 0xFF);
    private static final Slice INVALID_UTF8_1 = wrappedBuffer(new byte[] {-127});
    private static final Slice INVALID_UTF8_2 = wrappedBuffer(new byte[] {50, -127, 52, 50});
    private static final byte CONTINUATION_BYTE = (byte) 0b1011_1111;
    private static final Slice EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE =
            wrappedBuffer(CONTINUATION_BYTE, (byte) 0xE2, (byte) 0x80, (byte) 0x83, CONTINUATION_BYTE);

    private static final List<Slice> VALID_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .add(STRING_OO)
            .add(EMPTY_SLICE)
            .add(STRING_APPLE)
            .add(STRING_OESTERREICH)
            .add(STRING_DULIOE_DULIOE)
            .add(STRING_FAITH_HOPE_LOVE)
            .add(STRING_NAIVE)
            .add(STRING_OO)
            .build();

    private static final List<Slice> INVALID_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .add(INVALID_SEQUENCE_TO_LOWER_EXPANDS)
            .add(INVALID_UTF8_1)
            .add(INVALID_UTF8_2)
            .add(EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE)
            .build();

    private static final List<Slice> ALL_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .addAll(VALID_UTF8_SEQUENCES)
            .addAll(INVALID_UTF8_SEQUENCES)
            .build();

    private static final int REPLACEMENT_CHARACTER_CODE_POINT = 0xFFFD;
    private static final List<Integer> TEST_CODE_POINTS = ImmutableList.<Integer>builder()
            .add(0)
            .add((int) 'a')
            .add(0xC0)
            .add(0xC1)
            .add(0xEF)
            .add(0xFE)
            .add(0xFF)
            .add(REPLACEMENT_CHARACTER_CODE_POINT - 1)
            .add(REPLACEMENT_CHARACTER_CODE_POINT)
            .add(0xFFFE)
            .add(0xFFFF)
            .add(0x10405)
            .build();

    @Test
    public void testToStringStatistics()
    {
        // ORIGINAL version only produces stats at the row group level
        assertThat(OrcMetadataReader.toStringStatistics(
                ORIGINAL,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimum("ant")
                        .setMaximum("cat")
                        .setSum(44)
                        .build(),
                false)).isNull();

        // having only sum should work for current version
        for (boolean isRowGroup : ImmutableList.of(true, false)) {
            assertThat(OrcMetadataReader.toStringStatistics(
                    ORC_HIVE_8732,
                    OrcProto.StringStatistics.newBuilder()
                            .setSum(45)
                            .build(),
                    isRowGroup)).isEqualTo(new StringStatistics(null, null, 45));
        }
        // and the ORIGINAL version row group stats (but not rolled up stats)
        assertThat(OrcMetadataReader.toStringStatistics(
                ORIGINAL,
                OrcProto.StringStatistics.newBuilder()
                        .setSum(45)
                        .build(),
                true)).isEqualTo(new StringStatistics(null, null, 45));

        // having only a min or max should work
        assertThat(OrcMetadataReader.toStringStatistics(
                ORC_HIVE_8732,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimum("ant")
                        .build(),
                true)).isEqualTo(new StringStatistics(utf8Slice("ant"), null, 0));
        assertThat(OrcMetadataReader.toStringStatistics(
                ORC_HIVE_8732,
                OrcProto.StringStatistics.newBuilder()
                        .setMaximum("cat")
                        .build(),
                true)).isEqualTo(new StringStatistics(null, utf8Slice("cat"), 0));

        // normal full stat
        assertThat(OrcMetadataReader.toStringStatistics(
                ORC_HIVE_8732,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimum("ant")
                        .setMaximum("cat")
                        .setSum(79)
                        .build(),
                true)).isEqualTo(new StringStatistics(utf8Slice("ant"), utf8Slice("cat"), 79));

        for (Slice prefix : ALL_UTF8_SEQUENCES) {
            for (int testCodePoint : TEST_CODE_POINTS) {
                Slice codePoint = codePointToUtf8(testCodePoint);
                for (Slice suffix : ALL_UTF8_SEQUENCES) {
                    Slice testValue = concatSlice(prefix, codePoint, suffix);
                    testStringStatisticsTruncation(testValue, ORIGINAL);
                    testStringStatisticsTruncation(testValue, ORC_HIVE_8732);
                }
            }
        }
    }

    private static void testStringStatisticsTruncation(Slice testValue, HiveWriterVersion version)
    {
        assertThat(OrcMetadataReader.toStringStatistics(
                version,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                        .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                        .setSum(79)
                        .build(),
                true)).isEqualTo(createExpectedStringStatistics(version, testValue, testValue, 79));
        assertThat(OrcMetadataReader.toStringStatistics(
                version,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                        .setSum(79)
                        .build(),
                true)).isEqualTo(createExpectedStringStatistics(version, testValue, null, 79));
        assertThat(OrcMetadataReader.toStringStatistics(
                version,
                OrcProto.StringStatistics.newBuilder()
                        .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                        .setSum(79)
                        .build(),
                true)).isEqualTo(createExpectedStringStatistics(version, null, testValue, 79));
    }

    private static StringStatistics createExpectedStringStatistics(HiveWriterVersion version, Slice min, Slice max, int sum)
    {
        return new StringStatistics(
                minStringTruncateToValidRange(min, version),
                maxStringTruncateToValidRange(max, version),
                sum);
    }

    @Test
    public void testMinStringTruncateAtFirstReplacementCharacter()
    {
        for (Slice prefix : VALID_UTF8_SEQUENCES) {
            for (Slice suffix : VALID_UTF8_SEQUENCES) {
                testMinStringTruncateAtFirstReplacementCharacter(prefix, suffix);
            }
        }
    }

    private static void testMinStringTruncateAtFirstReplacementCharacter(Slice prefix, Slice suffix)
    {
        for (int testCodePoint : TEST_CODE_POINTS) {
            Slice codePoint = codePointToUtf8(testCodePoint);

            Slice value = concatSlice(prefix, codePoint, suffix);
            assertThat(minStringTruncateToValidRange(value, ORC_HIVE_8732)).isEqualTo(value);

            // For ORIGINAL, skip prefixes that truncate
            if (prefix.equals(minStringTruncateToValidRange(prefix, ORIGINAL))) {
                if (testCodePoint == REPLACEMENT_CHARACTER_CODE_POINT || testCodePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                    // truncate at test code point
                    assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(prefix);
                }
                else {
                    // truncate in suffix (if at all)
                    assertThat(minStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(concatSlice(prefix, codePoint, minStringTruncateToValidRange(suffix, ORIGINAL)));
                }
            }
        }
    }

    @Test
    public void testMaxStringTruncateAtFirstReplacementCharacter()
    {
        for (Slice prefix : VALID_UTF8_SEQUENCES) {
            for (Slice suffix : VALID_UTF8_SEQUENCES) {
                testMaxStringTruncateAtFirstReplacementCharacter(prefix, suffix);
            }
        }
    }

    private static void testMaxStringTruncateAtFirstReplacementCharacter(Slice prefix, Slice suffix)
    {
        for (int testCodePoint : TEST_CODE_POINTS) {
            Slice codePoint = codePointToUtf8(testCodePoint);

            Slice value = concatSlice(prefix, codePoint, suffix);
            assertThat(maxStringTruncateToValidRange(value, ORC_HIVE_8732)).isEqualTo(value);

            // For ORIGINAL, skip prefixes that truncate
            if (prefix.equals(maxStringTruncateToValidRange(prefix, ORIGINAL))) {
                if (testCodePoint == REPLACEMENT_CHARACTER_CODE_POINT || testCodePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                    // truncate at test code point
                    assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(concatSlice(prefix, wrappedBuffer((byte) 0xFF)));
                }
                else {
                    // truncate in suffix (if at all)
                    assertThat(maxStringTruncateToValidRange(value, ORIGINAL)).isEqualTo(concatSlice(prefix, codePoint, maxStringTruncateToValidRange(suffix, ORIGINAL)));
                }
            }
        }
    }

    private static Slice concatSlice(Slice... slices)
    {
        int totalLength = Arrays.stream(slices)
                .mapToInt(Slice::length)
                .sum();
        Slice value = Slices.allocate(totalLength);
        SliceOutput output = value.getOutput();
        for (Slice slice : slices) {
            output.writeBytes(slice);
        }
        return value;
    }
}
