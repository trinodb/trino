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

import io.airlift.log.Logger;

import java.io.ByteArrayOutputStream;

import static com.google.common.base.CharMatcher.javaIsoControl;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnsynchronizedOverridesSynchronized")
final class LoggingOutputStream
        extends ByteArrayOutputStream
{
    private final Logger logger;

    public LoggingOutputStream(Logger logger)
    {
        this.logger = requireNonNull(logger, "logger is null");
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        if (logger.isDebugEnabled()) {
            super.write(b, off, len);
            flush();
        }
    }

    @Override
    public void flush()
    {
        if (count > 4096) {
            log(toString(UTF_8));
            reset();
            return;
        }

        int index;
        for (index = count - 1; index >= 0; index--) {
            if (buf[index] == '\n') {
                break;
            }
        }
        if (index == -1) {
            return;
        }

        String data = new String(buf, 0, index, UTF_8);
        data.lines().forEach(this::log);

        int remaining = count - index - 1;
        System.arraycopy(buf, index + 1, buf, 0, remaining);
        count = remaining;
    }

    @Override
    public void close()
    {
        log(toString(UTF_8));
        reset();
    }

    private void log(String message)
    {
        String value = javaIsoControl().removeFrom(message).strip();
        if (!value.isEmpty()) {
            logger.debug(value);
        }
    }
}
