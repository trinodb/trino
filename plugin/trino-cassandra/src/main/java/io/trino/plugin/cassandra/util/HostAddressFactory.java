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
package io.trino.plugin.cassandra.util;

import com.datastax.oss.driver.api.core.metadata.Node;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

public class HostAddressFactory
{
    private final Map<String, HostAddress> hostMap = new HashMap<>();

    public HostAddress toHostAddress(Node node)
    {
        SocketAddress address = node.getEndPoint().resolve();
        if (address instanceof InetSocketAddress) {
            return toHostAddress(((InetSocketAddress) address).getAddress().getHostAddress());
        }
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR,
                format(
                        "Only endpoints which resolve to a InetSocketAddress are supported. Resolving to socket addresses of type %s is not supported",
                        address.getClass().getName()));
    }

    public List<HostAddress> toHostAddressList(Collection<Node> nodes)
    {
        ArrayList<HostAddress> list = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            list.add(toHostAddress(node));
        }
        return list;
    }

    public HostAddress toHostAddress(String hostAddressName)
    {
        HostAddress address = hostMap.get(hostAddressName);
        if (address == null) {
            address = HostAddress.fromString(hostAddressName);
            hostMap.put(hostAddressName, address);
        }
        return address;
    }

    public List<HostAddress> hostAddressNamesToHostAddressList(Collection<String> hosts)
    {
        ArrayList<HostAddress> list = new ArrayList<>(hosts.size());
        for (String host : hosts) {
            list.add(toHostAddress(host));
        }
        return list;
    }
}
