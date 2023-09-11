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

import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class IpAddressFunctions
{
    private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

    private IpAddressFunctions() {}

    @Description("Determines whether given IP address exists in the CIDR")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean contains(@SqlType(StandardTypes.VARCHAR) Slice network, @SqlType(StandardTypes.IPADDRESS) Slice address)
    {
        String cidr = network.toStringUtf8();

        int separator = cidr.indexOf("/");
        if (separator == -1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        String cidrBase = cidr.substring(0, separator);
        InetAddress cidrAddress;
        try {
            cidrAddress = InetAddresses.forString(cidrBase);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid network IP address");
        }

        byte[] cidrBytes = toBytes(cidrAddress);

        int prefixLength = Integer.parseInt(cidr.substring(separator + 1));
        if (prefixLength < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid prefix length");
        }

        // We do regex match instead of instanceof Inet4Address because InetAddresses.forString() normalizes
        // IPv4 mapped IPv6 addresses (e.g., ::ffff:0102:0304) to Inet4Address. We need to be able to
        // distinguish between the two formats in the CIDR string to be able to interpret the prefix length correctly.
        if (IPV4_PATTERN.matcher(cidrBase).matches()) {
            if (!isValidIpV4Cidr(cidrBytes, 12, prefixLength)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
            }
            prefixLength += 96;
        }
        else if (!isValidIpV6Cidr(prefixLength)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        if (prefixLength == 0) {
            return true;
        }

        byte[] ipAddress = address.getBytes();
        BigInteger cidrPrefix = new BigInteger(cidrBytes).shiftRight(cidrBytes.length * Byte.SIZE - prefixLength);
        BigInteger addressPrefix = new BigInteger(ipAddress).shiftRight(ipAddress.length * Byte.SIZE - prefixLength);

        return cidrPrefix.equals(addressPrefix);
    }

    private static boolean isValidIpV6Cidr(int prefixLength)
    {
        return prefixLength >= 0 && prefixLength <= 128;
    }

    private static boolean isValidIpV4Cidr(byte[] address, int offset, int prefix)
    {
        if (prefix < 0 || prefix > 32) {
            return false;
        }

        long mask = 0xFFFFFFFFL >>> prefix;
        return (Ints.fromBytes(address[offset], address[offset + 1], address[offset + 2], address[offset + 3]) & mask) == 0;
    }

    private static byte[] toBytes(InetAddress address)
    {
        byte[] bytes = address.getAddress();

        if (address instanceof Inet4Address) {
            byte[] temp = new byte[16];
            // IPv4 mapped addresses are encoded as ::ffff:<address>
            temp[10] = (byte) 0xFF;
            temp[11] = (byte) 0xFF;
            System.arraycopy(bytes, 0, temp, 12, 4);

            bytes = temp;
        }

        return bytes;
    }
}
