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
import java.net.UnknownHostException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class IpAddressFunctions
{
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

        byte[] base;
        boolean isIpv4;
        try {
            InetAddress inetAddress = InetAddresses.forString(cidr.substring(0, separator));
            base = inetAddress.getAddress();
            isIpv4 = inetAddress instanceof Inet4Address;
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid network IP address");
        }

        int prefixLength = Integer.parseInt(cidr.substring(separator + 1));

        if (prefixLength < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid prefix length");
        }

        int baseLength = base.length * Byte.SIZE;

        if (prefixLength > baseLength) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Prefix length exceeds address length");
        }

        if (isIpv4 && !isValidIpV4Cidr(base, prefixLength)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        if (!isIpv4 && !isValidIpV6Cidr(prefixLength)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        byte[] ipAddress;
        try {
            ipAddress = InetAddress.getByAddress(address.getBytes()).getAddress();
        }
        catch (UnknownHostException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid IP address");
        }

        if (base.length != ipAddress.length) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "IP address version should be the same");
        }

        if (prefixLength == 0) {
            return true;
        }

        BigInteger cidrPrefix = new BigInteger(base).shiftRight(baseLength - prefixLength);
        BigInteger addressPrefix = new BigInteger(ipAddress).shiftRight(baseLength - prefixLength);

        return cidrPrefix.equals(addressPrefix);
    }

    private static boolean isValidIpV6Cidr(int prefixLength)
    {
        return prefixLength >= 0 && prefixLength <= 128;
    }

    private static boolean isValidIpV4Cidr(byte[] address, int prefix)
    {
        long mask = 0xFFFFFFFFL >>> prefix;
        return (Ints.fromByteArray(address) & mask) == 0;
    }
}
