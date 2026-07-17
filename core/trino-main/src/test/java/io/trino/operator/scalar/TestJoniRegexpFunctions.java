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

import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.type.JoniRegexp;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpCount;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpExtract;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpExtractAll;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpLike;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpPosition;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpReplace;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpSplit;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.RegexLibrary.JONI;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJoniRegexpFunctions
        extends AbstractTestRegexpFunctions
{
    public TestJoniRegexpFunctions()
    {
        super(JONI);
    }

    @Test
    public void testSliceWithNonZeroOffset()
    {
        // A slice read out of a block is a view into a shared buffer, so its byteArrayOffset is not
        // zero. Joni takes the search bounds as offsets into that buffer, but reports match positions
        // relative to the start of the source, and mixing the two up silently returns wrong values.
        // A literal produces a slice at offset zero, where the two are the same and the mistake is
        // invisible, so match against slices at a non zero offset too.
        for (String value : new String[] {"", "a", "abc", "abcabc", "aXbXc", "123abc456", "über 42 straße", "aaaaaaaaaaaaaaaa"}) {
            for (String pattern : new String[] {"a", "[0-9]+", "(a)(b)", "X", "", "a+", "(?<name>b)"}) {
                assertSameAtAnyOffset(value, pattern);
            }
        }
    }

    private static void assertSameAtAnyOffset(String value, String pattern)
    {
        Slice plain = utf8Slice(value);
        Slice shifted = sliceAtNonZeroOffset(value);
        JoniRegexp regex = joniRegexp(utf8Slice(pattern));

        String context = "value='%s' pattern='%s'".formatted(value, pattern);
        if (!value.isEmpty()) {
            // an empty slice is shared and always sits at offset zero, so only assert this for the rest
            assertThat(shifted.byteArrayOffset()).describedAs(context).isPositive();
        }
        assertThat(shifted).describedAs(context).isEqualTo(plain);

        assertThat(regexpLike(shifted, regex))
                .describedAs("regexpLike %s", context)
                .isEqualTo(regexpLike(plain, regex));
        assertThat(regexpCount(shifted, regex))
                .describedAs("regexpCount %s", context)
                .isEqualTo(regexpCount(plain, regex));
        assertThat(regexpPosition(shifted, regex))
                .describedAs("regexpPosition %s", context)
                .isEqualTo(regexpPosition(plain, regex));
        assertThat(regexpExtract(shifted, regex))
                .describedAs("regexpExtract %s", context)
                .isEqualTo(regexpExtract(plain, regex));
        assertThat(regexpReplace(shifted, regex, utf8Slice("<$0>")))
                .describedAs("regexpReplace %s", context)
                .isEqualTo(regexpReplace(plain, regex, utf8Slice("<$0>")));
        assertBlockEquals(regexpSplit(shifted, regex), regexpSplit(plain, regex), "regexpSplit " + context);
        assertBlockEquals(regexpExtractAll(shifted, regex), regexpExtractAll(plain, regex), "regexpExtractAll " + context);
    }

    private static void assertBlockEquals(Block actual, Block expected, String context)
    {
        assertThat(actual.getPositionCount()).describedAs(context).isEqualTo(expected.getPositionCount());
        for (int position = 0; position < expected.getPositionCount(); position++) {
            assertThat(actual.isNull(position)).describedAs(context).isEqualTo(expected.isNull(position));
            if (!expected.isNull(position)) {
                assertThat(VARCHAR.getSlice(actual, position))
                        .describedAs("%s position %s", context, position)
                        .isEqualTo(VARCHAR.getSlice(expected, position));
            }
        }
    }

    /**
     * Returns a slice holding {@code value} that starts part way into its backing array, the way a
     * slice read out of a block does.
     */
    private static Slice sliceAtNonZeroOffset(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        byte[] padded = new byte[bytes.length + 8];
        Arrays.fill(padded, (byte) '#');
        System.arraycopy(bytes, 0, padded, 5, bytes.length);
        return Slices.wrappedBuffer(padded, 5, bytes.length);
    }

    @Test
    public void testSearchInterruptible()
            throws IOException, InterruptedException
    {
        String source = Resources.toString(Resources.getResource("regularExpressionExtraLongSource.txt"), UTF_8);
        String pattern = "\\((.*,)+(.*\\))";
        // Test the interruptible version of `Matcher#search` by "REGEXP_REPLACE"
        testJoniRegexpFunctionsInterruptible(() -> regexpReplace(utf8Slice(source), joniRegexp(utf8Slice(pattern))));
    }

    private static void testJoniRegexpFunctionsInterruptible(Runnable joniRegexpRunnable)
            throws InterruptedException
    {
        AtomicReference<TrinoException> trinoException = new AtomicReference<>();
        Thread searchChildThread = new Thread(() -> {
            try {
                joniRegexpRunnable.run();
            }
            catch (TrinoException e) {
                trinoException.compareAndSet(null, e);
            }
        });

        searchChildThread.start();

        // wait for the child thread to make some progress
        searchChildThread.join(1000);
        searchChildThread.interrupt();

        // wait for child thread to get in to terminated state
        searchChildThread.join();
        assertThat(trinoException.get()).isNotNull();
        assertThat(trinoException.get().getErrorCode()).isEqualTo(GENERIC_USER_ERROR.toErrorCode());
    }
}
