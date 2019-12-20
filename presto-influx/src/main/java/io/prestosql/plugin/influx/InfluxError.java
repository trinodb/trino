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

import java.util.Arrays;

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

    public void check(boolean condition, String message, Object... context)
    {
        if (!condition) {
            fail(message, getContextString(context));
        }
    }

    public void fail(String message, Object... context)
    {
        throw new PrestoException(this, message + getContextString(context));
    }

    public void fail(Throwable t)
    {
        throw new PrestoException(this, t);
    }

    private static String getContextString(Object[] context)
    {
        if (context == null || context.length == 0) {
            return "";
        }
        return " " + String.join(" ", Arrays.stream(context).toString());
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
