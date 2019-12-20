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

package io.prestosql.plugin.influx;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.PrestoException;

import static com.google.common.base.MoreObjects.toStringHelper;

public enum InfluxError
    implements ErrorCodeSupplier
{

    GENERAL(ErrorType.INTERNAL_ERROR),
    EXTERNAL(ErrorType.EXTERNAL),
    IDENTIFIER_CASE_SENSITIVITY(ErrorType.EXTERNAL),
    BAD_VALUE(ErrorType.USER_ERROR);

    private static final int ERROR_BASE = 0;  // FIXME needs allocating
    private final ErrorCode errorCode;

    InfluxError(ErrorType type)
    {
        this.errorCode = new ErrorCode(ERROR_BASE + ordinal(), name(), type);
    }

    public void check(boolean condition, String message, String context)
    {
        if (!condition) {
            fail(message, context);
        }
    }

    public void check(boolean condition, String message)
    {
        check(condition, message, null);
    }

    public void fail(String message, String context)
    {
        throw new PrestoException(this, message + (context != null && !context.isEmpty() ? " " + context : ""));
    }

    public void fail(String message)
    {
        fail(message, null);
    }

    public void fail(Throwable t)
    {
        throw new PrestoException(this, t);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("code", errorCode)
            .toString();
    }
}
