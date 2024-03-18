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
package io.varada.log;

import io.airlift.log.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShapingLoggerTest
{
    private Logger logger;

    @BeforeEach
    public void beforeEach()
    {
        logger = mock(Logger.class);
    }

    @Test
    public void testSimple()
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(logger, 2, Duration.ZERO);

        shapingLogger.debug("test");
        verify(logger, never()).debug(eq("test"));

        when(logger.isDebugEnabled()).thenReturn(true);
        shapingLogger.debug("test");
        shapingLogger.debug("test");
        verify(logger, times(1)).debug(eq("test"));
        verify(logger, times(1)).info(eq("test - skipped 1 times"));
    }

    @Test
    public void testSimpleWithFirstOccurrences()
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(logger, 5, Duration.ZERO, 3);

        when(logger.isDebugEnabled()).thenReturn(true);
        IntStream.range(0, 12).forEach((i) -> shapingLogger.debug("test"));

        verify(logger, times(8)).debug(eq("test"));
        verify(logger, times(2)).info(eq("test - skipped 2 times"));
    }

    @Test
    public void testSecondShouldNotPrint()
    {
        when(logger.isInfoEnabled()).thenReturn(true);

        ShapingLogger shapingLogger = ShapingLogger.getInstance(logger, 5, Duration.ZERO);

        shapingLogger.info("test");
        shapingLogger.info("test");
        shapingLogger.info("test");
        shapingLogger.info("test");
        shapingLogger.info("test");
        verify(logger, times(1))
                .info(eq("test"));
    }

    @Test
    public void testAfterThresholdShouldPrint()
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(logger, 2, Duration.ZERO);
        shapingLogger.warn("test");
        shapingLogger.warn("test");
        shapingLogger.warn("test");
        verify(logger, times(2))
                .warn(eq("test"));
    }

    @Test
    public void testAfterShapingIntervalLogFirst()
            throws InterruptedException
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                0,
                Duration.ofMillis(100));
        shapingLogger.error("test");

        Thread.sleep(130);

        shapingLogger.error("test");

        verify(logger, times(1)).error(eq("test"));

        shapingLogger.error("test");

        Thread.sleep(130);

        shapingLogger.error("test");

        verify(logger, times(2)).error(eq("test"));
    }

    @Test
    public void testAfterShapingIntervalLogNSamples()
            throws InterruptedException
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                10,
                Duration.ofMillis(100),
                2);
        IntStream.range(0, 5).forEach((i) -> shapingLogger.error("test"));

        Thread.sleep(130);
        IntStream.range(0, 5).forEach((i) -> shapingLogger.error("test"));

        verify(logger, times(4)).error(eq("test"));
        verify(logger, times(1)).info(eq("test - skipped 4 times"));
    }

    @Test
    public void testAfterShapingIntervalDontLogFirst()
            throws InterruptedException
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                0,
                Duration.ofMillis(100));
        shapingLogger.error("test");

        Thread.sleep(130);

        shapingLogger.error("test");

        verify(logger, times(1)).error(eq("test"));

        shapingLogger.error("test");

        Thread.sleep(130);

        shapingLogger.error("test");

        verify(logger, times(2)).error(eq("test"));
    }

    @Test
    public void testTwoCallsShouldReturnSameInstance()
    {
        ShapingLogger shapingLogger1 = ShapingLogger.getInstance(logger, 10, Duration.ofMillis(3));
        ShapingLogger shapingLogger2 = ShapingLogger.getInstance(logger, 10, Duration.ofMillis(3));
        assertSame(shapingLogger1, shapingLogger2);
    }

    @Test
    public void testByFormattedMessage()
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                10,
                Duration.ZERO,
                2,
                ShapingLogger.MODE.FULL);

        IntStream.range(0, 120).forEach(i -> shapingLogger.warn("Message %d", i % 3));
        verify(logger, times(8))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(0));
        verify(logger, times(8))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(1));
        verify(logger, times(8))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(2));
    }

    @Test
    public void testByFormattedMessageWithoutLogFirst()
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                10,
                Duration.ZERO,
                1,
                ShapingLogger.MODE.FULL);

        IntStream.range(0, 120).forEach(i -> shapingLogger.warn("Message %d", i % 3));
        verify(logger, times(4))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(0));
        verify(logger, times(4))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(1));
        verify(logger, times(4))
                .warn(nullable(Throwable.class), eq("Message %d"), eq(2));
    }

    @Test
    public void testConcurrentByFormattedMessage()
            throws InterruptedException
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                10,
                Duration.ZERO,
                2,
                ShapingLogger.MODE.FULL);

        Thread thread1 = new Thread(() -> IntStream.range(0, 50).forEach(i -> shapingLogger.error("Message %d", i % 2)));

        Thread thread2 = new Thread(() -> IntStream.range(0, 70).forEach(i -> shapingLogger.error("Message %d", i % 2)));

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        verify(logger, times(12))
                .error(nullable(Throwable.class), eq("Message %d"), eq(0));
        verify(logger, times(12))
                .error(nullable(Throwable.class), eq("Message %d"), eq(1));
    }

    @Test
    public void testConcurrentByFormat()
            throws InterruptedException
    {
        ShapingLogger shapingLogger = ShapingLogger.getInstance(
                logger,
                10,
                Duration.ZERO,
                2,
                ShapingLogger.MODE.FORMAT);

        Thread thread1 = new Thread(() -> {
            IntStream.range(0, 25).forEach(i -> shapingLogger.warn("Message %d", 123));
            IntStream.range(0, 25).forEach(i -> shapingLogger.warn("Message %d", 456));
        });

        Thread thread2 = new Thread(() -> {
            IntStream.range(0, 35).forEach(i -> shapingLogger.warn("Message %d", 123));
            IntStream.range(0, 35).forEach(i -> shapingLogger.warn("Message %d", 456));
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        verify(logger, times(24))
                .warn(nullable(Throwable.class), eq("Message %d"), anyInt());
    }
}
