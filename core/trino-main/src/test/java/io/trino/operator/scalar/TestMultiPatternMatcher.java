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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Arrays.fill;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMultiPatternMatcher
{
    @Test
    public void testCacheComparesPatternBoundaries()
    {
        MultiPatternMatcher matcher = createMatcher();
        Block first = patterns("a", "bc");
        Block sameContents = patterns("a", "bc");
        Block differentBoundaries = patterns("ab", "c");
        assertThat(matcher.matches(utf8Slice("a"), first)).isTrue();
        assertThat(matcher.matches(utf8Slice("a"), sameContents)).isTrue();
        assertThat(matcher.matches(utf8Slice("a"), differentBoundaries)).isFalse();
        assertThat(matcher.matches(utf8Slice("c"), differentBoundaries)).isTrue();
        assertThat(matcher.matches(utf8Slice("c"), patterns())).isFalse();
        assertThat(matcher.matches(utf8Slice("c"), first)).isFalse();
    }

    @Test
    public void testCacheComparesEncodedBlocksByLogicalPosition()
    {
        MultiPatternMatcher matcher = createMatcher();
        assertThat(matcher.matches(utf8Slice("alpha"), patterns("^alpha$", "^beta$"))).isTrue();
        Block dictionary = DictionaryBlock.create(2, patterns("^alpha$", "^beta$", "^gamma$"), new int[] {2, 0});
        assertThat(matcher.matches(utf8Slice("beta"), dictionary)).isFalse();
        assertThat(matcher.matches(utf8Slice("gamma"), dictionary)).isTrue();
        Block repeated = RunLengthEncodedBlock.create(patterns("^beta$"), 2);
        assertThat(matcher.matches(utf8Slice("gamma"), repeated)).isFalse();
        assertThat(matcher.matches(utf8Slice("beta"), repeated)).isTrue();
    }

    @Test
    public void testFailureDoesNotPoisonCache()
    {
        MultiPatternMatcher matcher = createMatcher();
        Block alpha = patterns("^alpha$", "^other$");
        assertThat(matcher.matches(utf8Slice("alpha"), alpha)).isTrue();
        Block invalid = patterns("^alpha$", "[");
        assertInvalid(matcher, utf8Slice("alpha"), invalid, "invalid regular expression at array index 2");
        assertInvalid(matcher, utf8Slice("alpha"), invalid, "invalid regular expression at array index 2");
        assertThat(matcher.matches(utf8Slice("alpha"), alpha)).isTrue();
        Block beta = patterns("^beta$", "^other$");
        assertThat(matcher.matches(utf8Slice("alpha"), beta)).isFalse();
        assertThat(matcher.matches(utf8Slice("beta"), beta)).isTrue();
    }

    @Test
    public void testSliceOffsets()
    {
        MultiPatternMatcher matcher = createMatcher();
        Block unicode = slicePatterns(offsetSlice("^é😀$"));
        assertThat(matcher.matches(offsetSlice("é😀"), unicode)).isTrue();
        assertThat(matcher.matches(offsetSlice("é"), unicode)).isFalse();
        assertThat(matcher.matches(offsetSlice(""), patterns("^$"))).isTrue();
        Block ending = patterns("z$");
        assertThat(matcher.matches(offsetSlice("a".repeat(100_000) + "z"), ending)).isTrue();
        assertThat(matcher.matches(offsetSlice("a"), ending)).isFalse();
        assertThat(matcher.matches(offsetSlice("a\0z"), ending)).isTrue();
    }

    @Test
    public void testMalformedUtf8()
    {
        List<Slice> malformed = List.of(
                wrappedBuffer(new byte[] {(byte) 0x80}),
                wrappedBuffer(new byte[] {(byte) 0xC0, (byte) 0xAF}),
                wrappedBuffer(new byte[] {(byte) 0xE2, (byte) 0x82}),
                wrappedBuffer(new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}),
                wrappedBuffer(new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}));
        MultiPatternMatcher matcher = createMatcher();
        for (Slice invalid : malformed) {
            assertInvalid(matcher, invalid, patterns(""), "source contains invalid UTF-8 at byte 0");
            assertInvalid(matcher, invalid, patterns(), "source contains invalid UTF-8 at byte 0");
            assertInvalid(matcher, utf8Slice(""), slicePatterns(utf8Slice(""), invalid), "pattern at array index 2 contains invalid UTF-8 at byte 0");
        }
        assertThat(matcher.matches(utf8Slice("é😀"), patterns("^..$"))).isTrue();
    }

    @Test
    public void testEncodedInputLimits()
    {
        MultiPatternMatcher matcher = createMatcher();
        assertThat(matcher.matches(utf8Slice(""), RunLengthEncodedBlock.create(patterns(""), 9_999))).isTrue();
        assertThat(matcher.matches(utf8Slice(""), RunLengthEncodedBlock.create(patterns(""), 10_000))).isTrue();
        assertInvalid(matcher, utf8Slice("a"), RunLengthEncodedBlock.create(patterns("a"), 10_001), "supports at most 10000 patterns");

        // Capture names count toward the encoded bytes without adding states to the automaton.
        Block maximumPattern = patterns("(?P<" + "n".repeat(65_529) + ">a)");
        assertThat(matcher.matches(utf8Slice("a"), patterns("(?P<" + "n".repeat(65_528) + ">a)"))).isTrue();
        assertThat(matcher.matches(utf8Slice("a"), maximumPattern)).isTrue();
        assertInvalid(matcher, utf8Slice("a"), patterns("é".repeat(32_768) + "a"), "pattern at array index 1 exceeds the 64 KiB UTF-8 limit");

        assertThat(matcher.matches(utf8Slice("a"), RunLengthEncodedBlock.create(maximumPattern, 15))).isTrue();
        assertThat(matcher.matches(utf8Slice("a"), RunLengthEncodedBlock.create(maximumPattern, 16))).isTrue();
        assertInvalid(matcher, utf8Slice("a"), RunLengthEncodedBlock.create(maximumPattern, 17), "patterns exceed the 1 MiB total UTF-8 limit");

        byte[] source = new byte[16 * 1024 * 1024 + 1];
        Block emptyPattern = patterns("");
        assertThat(matcher.matches(wrappedBuffer(source, 0, source.length - 2), emptyPattern)).isTrue();
        assertThat(matcher.matches(wrappedBuffer(source, 0, source.length - 1), emptyPattern)).isTrue();
        assertInvalid(matcher, wrappedBuffer(source), emptyPattern, "source exceeds the 16 MiB UTF-8 limit");
    }

    @Test
    public void testMatchingWithLowDfaStatesLimit()
    {
        MultiPatternMatcher matcher = new MultiPatternMatcher(2, 0);
        Block patterns = patterns("^[ab]{5}x$", "^z+$");
        assertThat(matcher.matches(utf8Slice("ababbx"), patterns)).isTrue();
        assertThat(matcher.matches(utf8Slice("ababby"), patterns)).isFalse();
        assertThat(matcher.matches(utf8Slice("zzz"), patterns)).isTrue();
    }

    private static MultiPatternMatcher createMatcher()
    {
        FeaturesConfig config = new FeaturesConfig();
        return new MultiPatternMatcher(config.getRe2JDfaStatesLimit(), config.getRe2JDfaRetries());
    }

    private static Slice offsetSlice(String value)
    {
        Slice bytes = utf8Slice(value);
        byte[] backing = new byte[bytes.length() + 8];
        fill(backing, (byte) 0xFF);
        System.arraycopy(bytes.byteArray(), bytes.byteArrayOffset(), backing, 3, bytes.length());
        return wrappedBuffer(backing, 3, bytes.length());
    }

    private static Block patterns(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeString(builder, value);
        }
        return builder.build();
    }

    private static Block slicePatterns(Slice... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (Slice value : values) {
            VARCHAR.writeSlice(builder, value);
        }
        return builder.build();
    }

    private static void assertInvalid(MultiPatternMatcher matcher, Slice source, Block patterns, String message)
    {
        assertTrinoExceptionThrownBy(() -> matcher.matches(source, patterns))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining(message);
    }
}
