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
package io.trino.version;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestEmbedVersion
{
    private EmbedVersion embedVersion;

    @BeforeAll
    public void setUp()
    {
        embedVersion = new EmbedVersion("123-some-test-version");
    }

    @AfterAll
    public void tearDown()
    {
        embedVersion = null;
    }

    @Test
    public void testEmbedVersionInRunnable()
    {
        AtomicInteger counter = new AtomicInteger();
        embedVersion.embedVersion((Runnable) counter::incrementAndGet).run();
        assertEquals(1, counter.get());

        assertThatThrownBy(() ->
                embedVersion.embedVersion((Runnable) () -> {
                    throw new RuntimeException("Zonky zonk");
                }).run())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Zonky zonk")
                .hasStackTraceContaining("at io.trino.$gen.Trino_123_some_test_version____");
    }

    @Test
    public void testEmbedVersionInCallable()
            throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        String value = embedVersion.embedVersion(() -> {
            return "abc" + counter.incrementAndGet();
        }).call();
        assertEquals("abc1", value);
        assertEquals(1, counter.get());

        assertThatThrownBy(() ->
                embedVersion.embedVersion((Callable<String>) () -> {
                    throw new RuntimeException("Zonky zonk");
                }).call())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Zonky zonk")
                .hasStackTraceContaining("at io.trino.$gen.Trino_123_some_test_version____")
                .hasNoCause();

        assertThatThrownBy(() ->
                embedVersion.embedVersion((Callable<String>) () -> {
                    throw new IOException("a checked exception");
                }).call())
                .isInstanceOf(IOException.class)
                .hasMessage("a checked exception")
                .hasStackTraceContaining("at io.trino.$gen.Trino_123_some_test_version____")
                .hasNoCause();
    }
}
