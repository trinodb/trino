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

import com.google.common.primitives.Bytes;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.type.UuidType.UUID;
import static java.util.UUID.nameUUIDFromBytes;
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

    @Description("Returns the nil UUID constant, which does not occur as a real UUID")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_nil()
    {
        java.util.UUID nilUuid = new UUID(0L, 0L);
        return wrappedLongArray(nilUuid.getMostSignificantBits(), nilUuid.getLeastSignificantBits());
    }

    @Description("Returns the DNS namespace constant for UUIDs")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_ns_dns()
    {
        String nameSpaceDns = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
        java.util.UUID nsDnsUuid = java.util.UUID.fromString(nameSpaceDns);
        return wrappedLongArray(nsDnsUuid.getMostSignificantBits(), nsDnsUuid.getLeastSignificantBits());
    }

    @Description("Returns the URL namespace constant for UUIDs")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_ns_url()
    {
        String nameSpaceUrl = "6ba7b811-9dad-11d1-80b4-00c04fd430c8";
        java.util.UUID nsUrlUuid = java.util.UUID.fromString(nameSpaceUrl);
        return wrappedLongArray(nsUrlUuid.getMostSignificantBits(), nsUrlUuid.getLeastSignificantBits());
    }

    @Description("Returns the ISO object identifier (OID) namespace constant for UUIDs")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_ns_oid()
    {
        String nameSpaceOid = "6ba7b812-9dad-11d1-80b4-00c04fd430c8";
        java.util.UUID nsOidUuid = java.util.UUID.fromString(nameSpaceOid);
        return wrappedLongArray(nsOidUuid.getMostSignificantBits(), nsOidUuid.getLeastSignificantBits());
    }

    @Description("Returns the X.500 distinguished name (DN) namespace constant for UUIDs")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_ns_x500()
    {
        String nameSpaceX500 = "6ba7b814-9dad-11d1-80b4-00c04fd430c8";
        java.util.UUID nsX500Uuid = java.util.UUID.fromString(nameSpaceX500);
        return wrappedLongArray(nsX500Uuid.getMostSignificantBits(), nsX500Uuid.getLeastSignificantBits());
    }

    @Description("Generates a deterministic (type 3) UUID using the specified namespace UUID and input name (using MD5 hash)")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_v3(@SqlType(StandardTypes.UUID) Slice nameSpaceUuid, @SqlType(StandardTypes.VARCHAR) Slice name)
    {
        byte[] value = name.getBytes();

        java.util.UUID nameUuid = new java.util.UUID(nameSpaceUuid.getLong(0), nameSpaceUuid.getLong(SIZE_OF_LONG));
        String uuidHexString = nameUuid.toString().replace("-", "");

        byte[] nameSpaceBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            int firstDigit = Character.digit(uuidHexString.substring(i * 2, i * 2 + 2).charAt(0), 16);
            int secondDigit = Character.digit(uuidHexString.substring(i * 2, i * 2 + 2).charAt(1), 16);
            byte b = (byte) ((firstDigit << 4) + secondDigit);
            nameSpaceBytes[i] = b;
        }

        java.util.UUID uuid = nameUUIDFromBytes(Bytes.concat(nameSpaceBytes, value));
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    @Description("Generates a deterministic (type 5) UUID using the specified namespace UUID and input name (using SHA hash)")
    @ScalarFunction
    @SqlType(StandardTypes.UUID)
    public static Slice uuid_v5(@SqlType(StandardTypes.UUID) Slice nameSpaceUuid, @SqlType(StandardTypes.VARCHAR) Slice name)
    {
        byte[] value = name.getBytes();

        java.util.UUID nameUuid = new java.util.UUID(nameSpaceUuid.getLong(0), nameSpaceUuid.getLong(SIZE_OF_LONG));
        String uuidHexString = nameUuid.toString().replace("-", "");

        byte[] nameSpaceBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            int firstDigit = Character.digit(uuidHexString.substring(i * 2, i * 2 + 2).charAt(0), 16);
            int secondDigit = Character.digit(uuidHexString.substring(i * 2, i * 2 + 2).charAt(1), 16);
            byte b = (byte) ((firstDigit << 4) + secondDigit);
            nameSpaceBytes[i] = b;
        }

        byte[] messageBytes = Bytes.concat(nameSpaceBytes, value);

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException nsae) {
            throw new InternalError("SHA-1 not supported", nsae);
        }

        byte[] sha1Bytes = Arrays.copyOfRange(md.digest(messageBytes), 0, 16);
        sha1Bytes[6] &= 0x0f; /* clear version        */
        sha1Bytes[6] |= 0x50; /* set to version 5     */
        sha1Bytes[8] &= 0x3f; /* clear variant        */
        sha1Bytes[8] |= 0x80; /* set to IETF variant  */

        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (sha1Bytes[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (sha1Bytes[i] & 0xff);
        }
        java.util.UUID uuid = new UUID(msb, lsb);
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.UUID) Slice left, @SqlType(StandardTypes.UUID) Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.UUID) Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.UUID) Slice value)
    {
        return XxHash64.hash(value);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.UUID)
    public static Slice castFromVarcharToUuid(@SqlType("varchar(x)") Slice slice)
    {
        try {
            java.util.UUID uuid = java.util.UUID.fromString(slice.toStringUtf8());
            if (slice.length() == 36) {
                return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
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
        return utf8Slice(new java.util.UUID(slice.getLong(0), slice.getLong(SIZE_OF_LONG)).toString());
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

    @ScalarOperator(IS_DISTINCT_FROM)
    public static final class UuidDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.UUID) Slice left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.UUID) Slice right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return notEqual(left, right);
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.UUID, nativeContainerType = Slice.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.UUID, nativeContainerType = Slice.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return !UUID.equalTo(left, leftPosition, right, rightPosition);
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.UUID) Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
