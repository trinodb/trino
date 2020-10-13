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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static java.lang.Long.reverseBytes;
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
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castFromVarcharToUuid(@SqlType("varchar(x)") Slice slice)
    {
        try {
            java.util.UUID uuid = java.util.UUID.fromString(slice.toStringUtf8());
            if (slice.length() == 36) {
                return wrappedLongArray(
                        reverseBytes(uuid.getMostSignificantBits()),
                        reverseBytes(uuid.getLeastSignificantBits()));
            }
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid UUID string length: " + slice.length());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to UUID: " + slice.toStringUtf8());
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromUuidToVarchar(@SqlType(StandardTypes.UUID) Slice slice)
    {
        long high = reverseBytes(slice.getLong(0));
        long low = reverseBytes(slice.getLong(SIZE_OF_LONG));
        return utf8Slice(new java.util.UUID(high, low).toString());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castFromVarbinaryToUuid(@SqlType("varbinary") Slice slice)
    {
        if (slice.length() == 16) {
            return slice;
        }
        throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid UUID binary length: " + slice.length());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castFromUuidToVarbinary(@SqlType(StandardTypes.UUID) Slice slice)
    {
        return wrappedBuffer(slice.getBytes());
    }
}
