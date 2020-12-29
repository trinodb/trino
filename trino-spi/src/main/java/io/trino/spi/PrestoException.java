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
package io.prestosql.spi;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoException
        extends RuntimeException
{
    private final ErrorCode errorCode;
    private final Optional<Location> location;

    public PrestoException(ErrorCodeSupplier errorCode, String message)
    {
        this(errorCode, message, null);
    }

    public PrestoException(ErrorCodeSupplier errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public PrestoException(ErrorCodeSupplier errorCode, String message, Throwable cause)
    {
        this(errorCode, Optional.empty(), message, cause);
    }

    public PrestoException(ErrorCodeSupplier errorCodeSupplier, Optional<Location> location, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.location = requireNonNull(location, "location is null");
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public Optional<Location> getLocation()
    {
        return location;
    }

    @Override
    public String getMessage()
    {
        String message = getRawMessage();
        if (location.isPresent()) {
            message = format("line %s:%s: %s", location.get().getLineNumber(), location.get().getColumnNumber(), message);
        }
        return message;
    }

    public String getRawMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
