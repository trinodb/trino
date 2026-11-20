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

import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static java.lang.System.arraycopy;

public final class IpAddressOperators
{
    private IpAddressOperators() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(IPADDRESS)
    public static Slice castFromVarcharToIpAddress(@SqlType("varchar(x)") Slice slice)
    {
        byte[] address;
        try {
            address = InetAddress.ofLiteral(slice.toStringUtf8()).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS: " + slice.toStringUtf8());
        }

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xFF;
            bytes[11] = (byte) 0xFF;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else if (address.length == 16) {
            bytes = address;
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromIpAddressToVarchar(@SqlType(IPADDRESS) Slice slice)
    {
        try {
            return utf8Slice(InetAddresses.toAddrString(InetAddress.getByAddress(slice.getBytes())));
        }
        catch (UnknownHostException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary length: " + slice.length(), e);
        }
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(IPADDRESS)
    public static Slice castFromIntegerToIpAddress(@SqlType(INTEGER) long value)
    {
        // Interpret all 32 bits as an unsigned IPv4 address.
        byte[] address = new byte[4];
        address[0] = (byte) (value >>> 24);
        address[1] = (byte) (value >>> 16);
        address[2] = (byte) (value >>> 8);
        address[3] = (byte) value;

        byte[] bytes = new byte[16];
        bytes[10] = (byte) 0xFF;
        bytes[11] = (byte) 0xFF;
        arraycopy(address, 0, bytes, 12, 4);
        return wrappedBuffer(bytes);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(IPADDRESS)
    public static Slice castFromBigintToIpAddress(@SqlType(BIGINT) long value)
    {
        // Interpret all 64 bits as an unsigned value and place them in the low 64 bits.
        byte[] bytes = new byte[16];
        bytes[8] = (byte) (value >>> 56);
        bytes[9] = (byte) (value >>> 48);
        bytes[10] = (byte) (value >>> 40);
        bytes[11] = (byte) (value >>> 32);
        bytes[12] = (byte) (value >>> 24);
        bytes[13] = (byte) (value >>> 16);
        bytes[14] = (byte) (value >>> 8);
        bytes[15] = (byte) value;
        return wrappedBuffer(bytes);
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlType(IPADDRESS)
    public static Slice castFromVarbinaryToIpAddress(@SqlType(VARBINARY) Slice slice)
    {
        if (slice.length() == 4) {
            byte[] address = slice.getBytes();
            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xFF;
            bytes[11] = (byte) 0xFF;
            arraycopy(address, 0, bytes, 12, 4);
            return wrappedBuffer(bytes);
        }
        if (slice.length() == 16) {
            return slice;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary length: " + slice.length());
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(VARBINARY)
    public static Slice castFromIpAddressToVarbinary(@SqlType(IPADDRESS) Slice slice)
    {
        return slice;
    }
}
