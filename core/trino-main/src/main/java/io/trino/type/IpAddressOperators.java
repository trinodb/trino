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
import static java.lang.System.arraycopy;

public final class IpAddressOperators
{
    private IpAddressOperators() {}

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.IPADDRESS)
    public static Slice castFromVarcharToIpAddress(@SqlType("varchar(x)") Slice slice)
    {
        byte[] address;
        try {
            address = InetAddresses.forString(slice.toStringUtf8()).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS: " + slice.toStringUtf8());
        }

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
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

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromIpAddressToVarchar(@SqlType(StandardTypes.IPADDRESS) Slice slice)
    {
        try {
            return utf8Slice(InetAddresses.toAddrString(InetAddress.getByAddress(slice.getBytes())));
        }
        catch (UnknownHostException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary length: " + slice.length(), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.IPADDRESS)
    public static Slice castFromVarbinaryToIpAddress(@SqlType("varbinary") Slice slice)
    {
        if (slice.length() == 4) {
            byte[] address = slice.getBytes();
            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
            return wrappedBuffer(bytes);
        }
        if (slice.length() == 16) {
            return slice;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary length: " + slice.length());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castFromIpAddressToVarbinary(@SqlType(StandardTypes.IPADDRESS) Slice slice)
    {
        return wrappedBuffer(slice.getBytes());
    }
}
