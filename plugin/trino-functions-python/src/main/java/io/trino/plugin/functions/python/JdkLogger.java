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
package io.trino.plugin.functions.python;

import com.dylibso.chicory.log.Logger;

import static java.util.Objects.requireNonNull;

final class JdkLogger
        implements Logger
{
    private final java.util.logging.Logger logger;

    public static Logger get(Class<?> clazz)
    {
        return new JdkLogger(java.util.logging.Logger.getLogger(clazz.getName()));
    }

    public JdkLogger(java.util.logging.Logger logger)
    {
        this.logger = requireNonNull(logger, "logger is null");
    }

    @Override
    public void log(Level level, String msg, Throwable throwable)
    {
        logger.log(toJdkLevel(level), msg, throwable);
    }

    @Override
    public boolean isLoggable(Level level)
    {
        return logger.isLoggable(toJdkLevel(level));
    }

    private static java.util.logging.Level toJdkLevel(Level level)
    {
        return switch (level) {
            case ALL -> java.util.logging.Level.ALL;
            case TRACE -> java.util.logging.Level.FINEST;
            case DEBUG -> java.util.logging.Level.FINE;
            case INFO -> java.util.logging.Level.INFO;
            case WARNING -> java.util.logging.Level.WARNING;
            case ERROR -> java.util.logging.Level.SEVERE;
            case OFF -> java.util.logging.Level.OFF;
        };
    }
}
