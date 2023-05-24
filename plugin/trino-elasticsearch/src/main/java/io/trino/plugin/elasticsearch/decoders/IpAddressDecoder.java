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
package io.trino.plugin.elasticsearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

public class IpAddressDecoder
        implements Decoder
{
    private final String path;
    private final Type ipAddressType;

    public IpAddressDecoder(String path, Type type)
    {
        this.path = requireNonNull(path, "path is null");
        this.ipAddressType = requireNonNull(type, "type is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String address) {
            Slice slice = castToIpAddress(Slices.utf8Slice(address));
            ipAddressType.writeSlice(output, slice);
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a string value for field '%s' of type IP: %s [%s]", path, value, value.getClass().getSimpleName()));
        }
    }

    // This is a copy of IpAddressOperators.castFromVarcharToIpAddress method
    private Slice castToIpAddress(Slice slice)
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

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;
        private final Type ipAddressType;

        @JsonCreator
        public Descriptor(String path, Type ipAddressType)
        {
            this.path = path;
            this.ipAddressType = ipAddressType;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public Type getIpAddressType()
        {
            return ipAddressType;
        }

        @Override
        public Decoder createDecoder()
        {
            return new IpAddressDecoder(path, ipAddressType);
        }
    }
}
