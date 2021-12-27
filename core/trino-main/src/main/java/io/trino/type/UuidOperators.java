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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.util.UUID.randomUUID;

public final class UuidOperators
{
    private UuidOperators() {}

    @Description("Generates a random UUID")
    @ScalarFunction(deterministic = false)
    @SqlType(StandardTypes.UUID)
    public static Slice uuid()
    {
        java.util.UUID uuid = randomUUID();
        return javaUuidToTrinoUuid(uuid);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castFromVarcharToUuid(@SqlType("varchar(x)") Slice slice)
    {
        try {
            java.util.UUID uuid = java.util.UUID.fromString(slice.toStringUtf8());
            if (slice.length() == 36) {
                return javaUuidToTrinoUuid(uuid);
            }
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid UUID string length: " + slice.length());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to UUID: " + slice.toStringUtf8());
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromUuidToVarchar(@SqlType(StandardTypes.UUID) Slice slice)
    {
        return utf8Slice(trinoUuidToJavaUuid(slice).toString());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castFromVarbinaryToUuid(@SqlType("varbinary") Slice slice)
    {
        if (slice.length() == 16) {
            return slice;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid UUID binary length: " + slice.length());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castFromUuidToVarbinary(@SqlType(StandardTypes.UUID) Slice slice)
    {
        return wrappedBuffer(slice.getBytes());
    }
}
