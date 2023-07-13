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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.JoniRegexpCasts.joniRegexp;
import static io.trino.operator.scalar.JoniRegexpFunctions.regexpReplace;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.sql.analyzer.RegexLibrary.JONI;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestJoniRegexpFunctions
        extends AbstractTestRegexpFunctions
{
    public TestJoniRegexpFunctions()
    {
        super(JONI);
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
        assertNotNull(trinoException.get());
        assertEquals(trinoException.get().getErrorCode(), GENERIC_USER_ERROR.toErrorCode());
    }
}
