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
import io.trino.re2j.Options;
import io.trino.re2j.Pattern;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;

import java.lang.ref.WeakReference;

import static io.airlift.slice.SliceUtf8.isAscii;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
import static io.trino.re2j.Options.Algorithm.DFA_FALLBACK_TO_NFA;
import static io.trino.re2j.Pattern.DOTALL;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class MultiPatternMatcher
{
    private static final int MAX_PATTERNS = 10_000;
    private static final int MAX_PATTERN_BYTES = 64 * 1024;
    private static final int MAX_TOTAL_PATTERN_BYTES = 1024 * 1024;
    private static final int MAX_SOURCE_BYTES = 16 * 1024 * 1024;

    private final Options options;
    private PatternKey patterns;
    private Pattern compiled;

    public MultiPatternMatcher(int dfaStatesLimit, int dfaRetries)
    {
        options = Options.builder()
                .setAlgorithm(DFA_FALLBACK_TO_NFA)
                .setMaximumNumberOfDFAStates(dfaStatesLimit)
                .setNumberOfDFARetries(dfaRetries)
                .build();
    }

    public synchronized boolean matches(Slice source, Block patternBlock)
    {
        if (source.length() > MAX_SOURCE_BYTES) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any source exceeds the 16 MiB UTF-8 limit");
        }
        validateUtf8(source, 0);
        if (patternBlock.getPositionCount() == 0) {
            patterns = null;
            compiled = null;
            return false;
        }
        if (patterns == null || !patterns.matches(patternBlock)) {
            // Drop the old automaton before compiling a potentially large replacement.
            patterns = null;
            compiled = null;
            PatternKey newPatterns = PatternKey.create(patternBlock);
            compiled = compile(newPatterns);
            patterns = newPatterns;
        }
        return compiled.find(source);
    }

    private Pattern compile(PatternKey patterns)
    {
        int count = patterns.offsets.length - 1;
        if (count == 1) {
            return compilePattern(new String(patterns.bytes, UTF_8), 1);
        }
        StringBuilder expression = new StringBuilder(patterns.bytes.length + count * 5);
        for (int index = 0; index < count; index++) {
            String pattern = new String(patterns.bytes, patterns.offsets[index], patterns.offsets[index + 1] - patterns.offsets[index], UTF_8);
            // A malformed entry must not participate in the syntax of another entry.
            compilePattern(pattern, index + 1);
            if (index > 0) {
                expression.append('|');
            }
            // Inline flags must not apply to other entries.
            expression.append("(?:").append(pattern);
            if (endsInQuote(pattern)) {
                expression.append("\\E");
            }
            expression.append(')');
        }
        try {
            return Pattern.compile(expression.toString(), DOTALL, options);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any could not compile combined regular expression", e);
        }
    }

    private Pattern compilePattern(String pattern, int index)
    {
        try {
            return Pattern.compile(pattern, DOTALL, options);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any invalid regular expression at array index " + index + ": " + e.getMessage(), e);
        }
    }

    private static boolean endsInQuote(String pattern)
    {
        // RE2J allows a quote to run to the end of the pattern. It must not consume the group's closing parenthesis.
        // The pattern has already been validated, so \\Q cannot occur as an escape in a character class.
        for (int index = pattern.indexOf('\\'); index >= 0 && index + 1 < pattern.length(); index = pattern.indexOf('\\', index + 2)) {
            if (pattern.charAt(index + 1) == 'Q') {
                int end = pattern.indexOf("\\E", index + 2);
                if (end < 0) {
                    return true;
                }
                index = end;
            }
        }
        return false;
    }

    // A zero array index identifies the source, not a pattern.
    private static void validateUtf8(Slice value, int arrayIndex)
    {
        if (isAscii(value)) {
            return;
        }
        byte[] bytes = value.byteArray();
        int offset = value.byteArrayOffset();
        int length = value.length();
        for (int position = 0; position < length; ) {
            int codePoint = tryGetCodePointAt(bytes, offset, length, position);
            if (codePoint < 0) {
                String argument = arrayIndex == 0 ? "source" : "pattern at array index " + arrayIndex;
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any " + argument + " contains invalid UTF-8 at byte " + position);
            }
            position += lengthOfCodePoint(codePoint);
        }
    }

    private static final class PatternKey
    {
        // Array identity is sufficient for immutable blocks, but the cache must not retain the input page.
        private final WeakReference<Block> original;
        private final byte[] bytes;
        private final int[] offsets;

        private PatternKey(Block original, byte[] bytes, int[] offsets)
        {
            this.original = new WeakReference<>(original);
            this.bytes = bytes;
            this.offsets = offsets;
        }

        static PatternKey create(Block block)
        {
            int count = block.getPositionCount();
            if (count > MAX_PATTERNS) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any supports at most 10000 patterns");
            }
            int[] offsets = new int[count + 1];
            int totalBytes = 0;
            for (int index = 0; index < count; index++) {
                if (block.isNull(index)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any pattern at array index " + (index + 1) + " is null");
                }
                Slice pattern = VARCHAR.getSlice(block, index);
                if (pattern.length() > MAX_PATTERN_BYTES) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any pattern at array index " + (index + 1) + " exceeds the 64 KiB UTF-8 limit");
                }
                totalBytes += pattern.length();
                if (totalBytes > MAX_TOTAL_PATTERN_BYTES) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "multi_match_any patterns exceed the 1 MiB total UTF-8 limit");
                }
                validateUtf8(pattern, index + 1);
                offsets[index + 1] = totalBytes;
            }
            byte[] bytes = new byte[totalBytes];
            for (int index = 0; index < count; index++) {
                Slice pattern = VARCHAR.getSlice(block, index);
                System.arraycopy(pattern.byteArray(), pattern.byteArrayOffset(), bytes, offsets[index], pattern.length());
            }
            return new PatternKey(block, bytes, offsets);
        }

        boolean matches(Block block)
        {
            if (original.refersTo(block)) {
                return true;
            }
            int count = offsets.length - 1;
            if (block.getPositionCount() != count) {
                return false;
            }
            VariableWidthBlock values = (VariableWidthBlock) block.getUnderlyingValueBlock();
            Slice rawSlice = values.getRawSlice();
            for (int index = 0; index < count; index++) {
                int position = block.getUnderlyingValuePosition(index);
                if (values.isNull(position)) {
                    return false;
                }
                int length = offsets[index + 1] - offsets[index];
                if (!rawSlice.equals(values.getRawSliceOffset(position), values.getSliceLength(position), bytes, offsets[index], length)) {
                    return false;
                }
            }
            return true;
        }
    }
}
