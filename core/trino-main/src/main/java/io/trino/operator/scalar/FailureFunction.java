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
package io.trino.operator.scalar;

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.client.FailureInfo;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public final class FailureFunction
{
    private static final JsonCodec<FailureInfo> JSON_CODEC = JsonCodec.jsonCodec(FailureInfo.class);

    private FailureFunction() {}

    // We shouldn't be using UNKNOWN as an explicit type. This will be fixed when we fix type inference
    @Description("Decodes json to an exception and throws it")
    @ScalarFunction(value = "fail", hidden = true)
    @SqlType("unknown")
    public static boolean failWithException(@SqlType(StandardTypes.JSON) Slice failureInfoSlice)
    {
        FailureInfo failureInfo = JSON_CODEC.fromJson(failureInfoSlice.getInput());
        // wrap the failure in a new exception to append the current stack trace
        throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, failureInfo.toException());
    }

    @Description("Throws an exception with a given message")
    @ScalarFunction(value = "fail", hidden = true)
    @SqlType("unknown")
    public static boolean fail(@SqlType(StandardTypes.VARCHAR) Slice message)
    {
        throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, message.toStringUtf8());
    }

    @Description("Throws an exception with a given error code and message")
    @ScalarFunction(value = "fail", hidden = true)
    @SqlType("unknown")
    public static boolean fail(
            @SqlType(StandardTypes.INTEGER) long errorCode,
            @SqlType(StandardTypes.VARCHAR) Slice message)
    {
        for (StandardErrorCode standardErrorCode : StandardErrorCode.values()) {
            if (standardErrorCode.toErrorCode().getCode() == errorCode) {
                throw new TrinoException(standardErrorCode, message.toStringUtf8());
            }
        }
        throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Unable to find error for code: " + errorCode);
    }
}
