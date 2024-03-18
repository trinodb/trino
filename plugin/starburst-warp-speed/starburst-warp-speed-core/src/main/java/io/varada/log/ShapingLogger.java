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

import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.varada.tools.util.Pair;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * this Logger counts similar logs and only outputs a single log message based on threshold / intervalInMillis
 */
public class ShapingLogger
{
    private static final Map<Logger, ShapingLogger> instances = new ConcurrentHashMap<>();

    private static final String FORMAT = "%s - skipped %d times";
    private final Map<Pair<String, List<Object>>, ShapingLoggerState> shapingLoggerStateMap = new ConcurrentHashMap<>();
    private final Logger logger;

    //threshold for log flushing. if threshold<=0 then threshold is ignored
    private final int threshold;

    //interval in millis for log flushing. if duration = Duration.ZERO then it is ignored
    private final Duration duration;

    //if shapeMode=FORMAT then we accumulate by format string
    //if shapeMode=FULL then we accumulate by formatted string
    private final MODE mode;

    //if true first log message will be logged regardless of interval/duration
    private final int numberOfSamples;

    private ShapingLogger(Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamples,
            MODE mode)
    {
        this.logger = requireNonNull(logger);
        this.threshold = threshold;
        this.duration = duration != null ? duration : Duration.ZERO;
        this.numberOfSamples = numberOfSamples;
        checkArgument(threshold == 0 || threshold > numberOfSamples, "threshold must be greater than number of samples");
        this.mode = requireNonNull(mode);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration)
    {
        return getInstance(logger, threshold, duration, 1);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamplings)
    {
        return getInstance(logger, threshold, duration, numberOfSamplings, MODE.FORMAT);
    }

    public static ShapingLogger getInstance(
            Logger logger,
            int threshold,
            Duration duration,
            int numberOfSamplings,
            MODE mode)
    {
        return instances.computeIfAbsent(logger, s -> new ShapingLogger(logger, threshold, duration, numberOfSamplings, mode));
    }

    public void debug(Throwable exception, String message)
    {
        if (logger.isDebugEnabled()) {
            log(getKey(message), () -> logger.debug(exception, message));
        }
    }

    public void debug(String message)
    {
        if (logger.isDebugEnabled()) {
            log(getKey(message), () -> logger.debug(message));
        }
    }

    @FormatMethod
    public void debug(String format, Object... args)
    {
        if (logger.isDebugEnabled()) {
            log(getKey(format, args), () -> logger.debug(format, args));
        }
    }

    @FormatMethod
    public void debug(Throwable exception, String format, Object... args)
    {
        if (logger.isDebugEnabled()) {
            log(getKey(format, args), () -> logger.debug(exception, format, args));
        }
    }

    public void info(String message)
    {
        if (logger.isInfoEnabled()) {
            log(getKey(message), () -> logger.info(message));
        }
    }

    @FormatMethod
    public void info(String format, Object... args)
    {
        if (logger.isInfoEnabled()) {
            log(getKey(format, args), () -> logger.info(format, args));
        }
    }

    public void warn(Throwable exception, String message)
    {
        log(getKey(message), () -> logger.warn(exception, message));
    }

    public void warn(String message)
    {
        log(getKey(message), () -> logger.warn(message));
    }

    @FormatMethod
    public void warn(final String format, Object... args)
    {
        warn(null, format, args);
    }

    @FormatMethod
    public void warn(Throwable exception, final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.warn(exception, format, args));
    }

    public void error(Throwable exception, String message)
    {
        log(getKey(message), () -> logger.error(exception, message));
    }

    public void error(String message)
    {
        log(getKey(message), () -> logger.error(message));
    }

    @FormatMethod
    public void error(final String format, Object... args)
    {
        error(null, format, args);
    }

    @FormatMethod
    public void error(Throwable e, final String format, Object... args)
    {
        log(getKey(format, args), () -> logger.error(e, format, args));
    }

    private void log(Pair<String, List<Object>> key, Runnable runnable)
    {
        shapingLoggerStateMap.compute(key, (k, val) -> {
            if (val == null) {
                val = new ShapingLoggerState(1, System.currentTimeMillis());
            }

            long lastLogTime = val.lastLogTime();
            int count = val.count();
            if (count <= numberOfSamples) {
                runnable.run();
            }

            if ((threshold > 0 && (count == threshold)) ||
                    ((duration.toMillis() > 0) && System.currentTimeMillis() - lastLogTime > duration.toMillis())) {
                if (count > numberOfSamples) {
                    logger.info(getOccurrencesMessage(key, count - numberOfSamples));
                }
                lastLogTime = System.currentTimeMillis();
                count = 0;
            }
            return new ShapingLoggerState(count + 1, lastLogTime);
        });
    }

    private Pair<String, List<Object>> getKey(String message, Object... args)
    {
        if (mode.equals(MODE.FORMAT)) {
            return Pair.of(message, List.of());
        }
        return Pair.of(message, Arrays.asList(args));
    }

    private String getOccurrencesMessage(Pair<String, List<Object>> key, int count)
    {
        String message = key.getValue().isEmpty() ? key.getKey() : key.getKey().formatted(key.getValue().toArray());
        return FORMAT.formatted(message, count);
    }

    private record ShapingLoggerState(int count, long lastLogTime) {}

    public enum MODE
    {
        FORMAT, FULL
    }
}
