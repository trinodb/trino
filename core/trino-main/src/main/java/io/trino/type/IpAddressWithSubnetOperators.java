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
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.IPADDRESS_WITH_SUBNET;
import static io.trino.type.IpAddressType.IPADDRESS;

public final class IpAddressWithSubnetOperators
{
    private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
    private static final int IPV4_PREFIX_BITS = 32;
    private static final int IPV6_PREFIX_BITS = 128;
    private static final int IPV4_MAPPED_OFFSET = 96;

    private IpAddressWithSubnetOperators() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(IPADDRESS_WITH_SUBNET)
    public static SqlRow castFromVarcharToIpAddressWithSubnet(@SqlType("varchar(x)") Slice slice)
    {
        String value = slice.toStringUtf8();
        int separator = value.lastIndexOf('/');
        if (separator < 0 || separator == 0 || separator == value.length() - 1) {
            throw invalidCast(value);
        }

        String addressString = value.substring(0, separator);
        String prefixString = value.substring(separator + 1);

        int prefixLength;
        try {
            prefixLength = Integer.parseInt(prefixString);
        }
        catch (NumberFormatException e) {
            throw invalidCast(value, e);
        }

        boolean ipv4 = IPV4_PATTERN.matcher(addressString).matches();
        int maxPrefixLength = ipv4 ? IPV4_PREFIX_BITS : IPV6_PREFIX_BITS;
        if (prefixLength < 0 || prefixLength > maxPrefixLength) {
            throw invalidCast(value);
        }

        Slice addressSlice;
        try {
            addressSlice = IpAddressOperators.castFromVarcharToIpAddress(utf8Slice(addressString));
        }
        catch (TrinoException e) {
            throw invalidCast(value, e);
        }

        // Normalize the value to the network address by clearing the host bits.
        int effectivePrefixLength = prefixLength + (ipv4 ? IPV4_MAPPED_OFFSET : 0);
        Slice networkSlice = wrappedBuffer(mask(addressSlice.getBytes(), effectivePrefixLength));

        return buildRowValue(IpAddressWithSubnetType.IPADDRESS_WITH_SUBNET, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), networkSlice);
            INTEGER.writeLong(fieldBuilders.get(1), prefixLength);
        });
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromIpAddressWithSubnetToVarchar(@SqlType(IPADDRESS_WITH_SUBNET) SqlRow row)
    {
        Slice addressSlice = IPADDRESS.getSlice(row.getRawFieldBlock(0), row.getRawIndex());
        long prefixLength = INTEGER.getLong(row.getRawFieldBlock(1), row.getRawIndex());

        return utf8Slice(formatAddress(addressSlice, (int) prefixLength) + "/" + prefixLength);
    }

    static byte[] mask(byte[] address, int prefixLength)
    {
        byte[] result = address.clone();
        int fullBytes = prefixLength / Byte.SIZE;
        int remainingBits = prefixLength % Byte.SIZE;

        if (remainingBits > 0) {
            result[fullBytes] &= (byte) (0xFF << (Byte.SIZE - remainingBits));
            fullBytes++;
        }

        for (int i = fullBytes; i < result.length; i++) {
            result[i] = 0;
        }
        return result;
    }

    static boolean isIpv4Mapped(byte[] address)
    {
        for (int i = 0; i < 10; i++) {
            if (address[i] != 0) {
                return false;
            }
        }
        return address[10] == (byte) 0xFF && address[11] == (byte) 0xFF;
    }

    private static String formatAddress(Slice addressSlice, int prefixLength)
    {
        byte[] address = addressSlice.getBytes();
        if (isIpv4Mapped(address) && prefixLength > IPV4_PREFIX_BITS) {
            try {
                return InetAddresses.toAddrString(Inet6Address.getByAddress("ipv6", address, -1));
            }
            catch (UnknownHostException e) {
                throw new IllegalStateException("Invalid IP address binary length: " + address.length, e);
            }
        }
        return IpAddressOperators.castFromIpAddressToVarchar(addressSlice).toStringUtf8();
    }

    private static TrinoException invalidCast(String value)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS_WITH_SUBNET: " + value);
    }

    private static TrinoException invalidCast(String value, Exception cause)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS_WITH_SUBNET: " + value, cause);
    }
}
