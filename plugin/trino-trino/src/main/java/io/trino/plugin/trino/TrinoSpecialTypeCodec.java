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
package io.trino.plugin.trino;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.type.UuidType;

import java.net.InetAddress;
import java.util.UUID;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

final class TrinoSpecialTypeCodec
{
    private TrinoSpecialTypeCodec() {}

    static Slice jsonSlice(String value)
    {
        if (value == null) {
            return null;
        }
        return Slices.utf8Slice(value);
    }

    static Slice uuidSlice(String value)
    {
        if (value == null) {
            return null;
        }
        return UuidType.javaUuidToTrinoUuid(UUID.fromString(value));
    }

    static Slice ipAddressSlice(String value)
    {
        if (value == null) {
            return null;
        }
        try {
            InetAddress address = InetAddress.ofLiteral(value);
            return Slices.wrappedBuffer(toCanonicalAddressBytes(address.getAddress()));
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(JDBC_ERROR, "Invalid IP address: " + value, e);
        }
    }

    private static byte[] toCanonicalAddressBytes(byte[] bytes)
    {
        if (bytes.length == 4) {
            byte[] ipv6 = new byte[16];
            ipv6[10] = (byte) 0xFF;
            ipv6[11] = (byte) 0xFF;
            System.arraycopy(bytes, 0, ipv6, 12, 4);
            return ipv6;
        }
        return bytes;
    }
}
