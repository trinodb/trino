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
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.IPADDRESS_WITH_PORT;
import static io.trino.type.IpAddressType.IPADDRESS;

public final class IpAddressWithPortOperators
{
    private static final int MAX_PORT = 65535;

    private IpAddressWithPortOperators() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(IPADDRESS_WITH_PORT)
    public static SqlRow castFromVarcharToIpAddressWithPort(@SqlType("varchar(x)") Slice slice)
    {
        String value = slice.toStringUtf8();
        String address;
        String portString;

        if (value.startsWith("[")) {
            // IPv6 address with port: [2001:db8::1]:443
            int closingBracket = value.indexOf(']');
            if (closingBracket < 0 || closingBracket + 1 >= value.length() || value.charAt(closingBracket + 1) != ':') {
                throw invalidCast(value);
            }
            address = value.substring(1, closingBracket);
            portString = value.substring(closingBracket + 2);
        }
        else {
            // IPv4 address, or an unbracketed IPv6 address, with port
            int colon = value.lastIndexOf(':');
            if (colon < 0) {
                throw invalidCast(value);
            }
            address = value.substring(0, colon);
            portString = value.substring(colon + 1);
        }

        int port;
        try {
            port = Integer.parseInt(portString);
        }
        catch (NumberFormatException e) {
            throw invalidCast(value, e);
        }
        if (port < 0 || port > MAX_PORT) {
            throw invalidCast(value);
        }

        Slice addressSlice;
        try {
            addressSlice = IpAddressOperators.castFromVarcharToIpAddress(utf8Slice(address));
        }
        catch (TrinoException e) {
            throw invalidCast(value, e);
        }

        return buildRowValue(IpAddressWithPortType.IPADDRESS_WITH_PORT, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), addressSlice);
            INTEGER.writeLong(fieldBuilders.get(1), port);
        });
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromIpAddressWithPortToVarchar(@SqlType(IPADDRESS_WITH_PORT) SqlRow row)
    {
        Slice addressSlice = IPADDRESS.getSlice(row.getRawFieldBlock(0), row.getRawIndex());
        long port = INTEGER.getLong(row.getRawFieldBlock(1), row.getRawIndex());

        String address = IpAddressOperators.castFromIpAddressToVarchar(addressSlice).toStringUtf8();
        if (address.indexOf(':') >= 0) {
            return utf8Slice("[" + address + "]:" + port);
        }
        return utf8Slice(address + ":" + port);
    }

    private static TrinoException invalidCast(String value)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS_WITH_PORT: " + value);
    }

    private static TrinoException invalidCast(String value, Exception cause)
    {
        return new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPADDRESS_WITH_PORT: " + value, cause);
    }
}
