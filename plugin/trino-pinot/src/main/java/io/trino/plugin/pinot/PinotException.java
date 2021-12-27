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
package io.trino.plugin.pinot;

import io.trino.spi.TrinoException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotException
        extends TrinoException
{
    private final boolean retryable;

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message)
    {
        this(errorCode, query, message, false, null);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, boolean retryable)
    {
        this(errorCode, query, message, retryable, null);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, Throwable throwable)
    {
        this(errorCode, query, message, false, throwable);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, boolean retryable, Throwable throwable)
    {
        super(requireNonNull(errorCode, "errorCode is null"), formatMessage(query, message), throwable);
        this.retryable = retryable;
    }

    public boolean isRetryable()
    {
        return retryable;
    }

    private static String formatMessage(Optional<String> query, String message)
    {
        requireNonNull(query, "query is null");
        requireNonNull(message, "message is null");

        if (query.isPresent()) {
            message += " with query \"" + query.get() + "\"";
        }
        return message;
    }
}
