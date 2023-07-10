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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.net.InetAddresses;
import com.google.inject.Inject;
import io.trino.spi.HostAddress;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static java.util.Objects.requireNonNull;

public class SubnetBasedTopology
        implements NetworkTopology
{
    private final List<byte[]> subnetMasks;
    private final AddressProtocol protocol;

    @Inject
    public SubnetBasedTopology(SubnetTopologyConfig config)
    {
        this(config.getCidrPrefixLengths(), config.getAddressProtocol());
    }

    public SubnetBasedTopology(List<Integer> cidrPrefixLengths, AddressProtocol protocol)
    {
        requireNonNull(cidrPrefixLengths, "cidrPrefixLengths is null");
        requireNonNull(protocol, "protocol is null");

        validateHierarchy(cidrPrefixLengths, protocol);

        this.protocol = protocol;
        this.subnetMasks = cidrPrefixLengths.stream()
                .map(protocol::computeSubnetMask)
                .collect(toImmutableList());
    }

    @Override
    public NetworkLocation locate(HostAddress address)
    {
        try {
            InetAddress inetAddress = protocol.getInetAddress(address.getAllInetAddresses());

            if (inetAddress == null) {
                return ROOT_LOCATION;
            }

            byte[] addressBytes = inetAddress.getAddress();
            ImmutableList.Builder<String> segments = ImmutableList.builder();

            for (byte[] subnetMask : subnetMasks) {
                byte[] bytes = applyMask(addressBytes, subnetMask);
                segments.add(InetAddresses.toAddrString(InetAddress.getByAddress(bytes)));
            }

            segments.add(InetAddresses.toAddrString(inetAddress));
            return new NetworkLocation(segments.build());
        }
        catch (UnknownHostException e) {
            return ROOT_LOCATION;
        }
    }

    private byte[] applyMask(byte[] addressBytes, byte[] subnetMask)
    {
        int length = subnetMask.length;
        byte[] subnet = new byte[length];
        for (int i = 0; i < length; i++) {
            subnet[i] = (byte) (addressBytes[i] & subnetMask[i]);
        }
        return subnet;
    }

    private static void validateHierarchy(List<Integer> lengths, AddressProtocol protocol)
    {
        if (!Ordering.natural().isStrictlyOrdered(lengths)) {
            throw new IllegalArgumentException("Subnet hierarchy should be listed in the order of increasing prefix lengths");
        }

        if (!lengths.isEmpty()) {
            if (lengths.get(0) <= 0 || Iterables.getLast(lengths) >= protocol.getTotalBitCount()) {
                throw new IllegalArgumentException("Subnet mask prefix lengths are invalid");
            }
        }
    }

    public enum AddressProtocol
    {
        IPv4(Inet4Address.class, 32),
        IPv6(Inet6Address.class, 128);

        private final Class<?> addressClass;
        private final int totalBitCount;

        AddressProtocol(Class<?> addressClass, int totalBitCount)
        {
            this.addressClass = addressClass;
            this.totalBitCount = totalBitCount;
        }

        int getTotalBitCount()
        {
            return totalBitCount;
        }

        // Compute a mask with n leading bits set
        byte[] computeSubnetMask(int n)
        {
            checkArgument(n > 0 && n < getTotalBitCount(), "Invalid length for subnet mask");
            byte[] mask = new byte[getTotalBitCount() / Byte.SIZE]; // default is zero

            for (int i = 0; i < mask.length; i++) {
                if (n < Byte.SIZE) {
                    mask[i] = (byte) -(1 << (Byte.SIZE - n)); // set n leading bits
                    break;
                }

                mask[i] = (byte) 0xff;
                n -= Byte.SIZE;
            }

            return mask;
        }

        InetAddress getInetAddress(List<InetAddress> inetAddresses)
        {
            return inetAddresses.stream()
                    .filter(addressClass::isInstance)
                    .findFirst()
                    .orElse(null);
        }
    }
}
