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
package io.prestosql.operator.scalar;

import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        int prefixLength = Integer.parseInt(cidr.substring(separator + 1));

        if (prefixLength < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid prefix length");
        }

        byte[] base;
        try {
            base = InetAddresses.forString(cidr.substring(0, separator)).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR");
        }

        int baseLength = base.length * Byte.SIZE;

        if (prefixLength > baseLength) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Prefix length exceeds address length");
        }

        BigInteger cidrPrefix = new BigInteger(base).shiftRight(baseLength - prefixLength);
        byte[] ipAddress;
        try {
            ipAddress = InetAddress.getByAddress(address.getBytes()).getAddress();
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid IP address");
        }

        // If the IPv6 address is the range of IPv4, it is changed to v4 internally
        if (base.length != ipAddress.length) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "IP address version should be the same");
        }

        if (prefixLength == 0) {
            return true;
        }

        BigInteger addressPrefix = new BigInteger(ipAddress).shiftRight(baseLength - prefixLength);

        return cidrPrefix.equals(addressPrefix);
    }
}
