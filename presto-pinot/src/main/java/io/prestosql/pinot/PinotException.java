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
package io.prestosql.pinot;

import io.prestosql.spi.PrestoException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotException
        extends PrestoException
{
    private final Optional<String> query;
    private final boolean retriable;

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message)
    {
        this(errorCode, query, message, false, null);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, boolean retriable)
    {
        this(errorCode, query, message, retriable, null);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, Throwable throwable)
    {
        this(errorCode, query, message, false, throwable);
    }

    public PinotException(PinotErrorCode errorCode, Optional<String> query, String message, boolean retriable, Throwable throwable)
    {
        super(requireNonNull(errorCode, "errorCode is null"), requireNonNull(message, "message is null"), throwable);
        this.retriable = retriable;
        this.query = requireNonNull(query, "query is null");
    }

    public boolean isRetriable()
    {
        return retriable;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (query.isPresent()) {
            message += " with query \"" + query.get() + "\"";
        }
        return message;
    }
}
