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
package io.trino.node;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.XxHash64;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeVersion;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.node.AddressToHostname.tryDecodeHostnameToAddress;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
public class InternalNode
        implements Node
{
    private final String nodeIdentifier;
    private final URI internalUri;
    private final NodeVersion nodeVersion;
    private final boolean coordinator;
    private final Set<String> nodeGroups;
    private final long longHashCode;

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator)
    {
        this(nodeIdentifier, internalUri, nodeVersion, coordinator, ImmutableSet.of());
    }

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator, Set<String> nodeGroups)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.internalUri = requireNonNull(internalUri, "internalUri is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.nodeGroups = ImmutableSet.copyOf(requireNonNull(nodeGroups, "nodeGroups is null"));
        // nodeGroups is deliberately excluded so that changing a node's groups does not reshuffle
        // rendezvous hashing of splits, which is keyed on this value
        this.longHashCode = new XxHash64(coordinator ? 1 : 0)
                .update(nodeIdentifier.getBytes(UTF_8))
                .update(internalUri.toString().getBytes(UTF_8))
                .update(nodeVersion.version().getBytes(UTF_8))
                .hash();
    }

    @Override
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String getHost()
    {
        return internalUri.getHost();
    }

    public URI getInternalUri()
    {
        return internalUri;
    }

    public InetAddress getInternalAddress()
            throws UnknownHostException
    {
        Optional<InetAddress> address = tryDecodeHostnameToAddress(internalUri.getHost());
        if (address.isPresent()) {
            return address.get();
        }
        return InetAddress.getByName(internalUri.getHost());
    }

    @Override
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(internalUri);
    }

    @Override
    public String getVersion()
    {
        return nodeVersion.version();
    }

    @Override
    public boolean isCoordinator()
    {
        return coordinator;
    }

    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    public Set<String> getNodeGroups()
    {
        return nodeGroups;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        InternalNode o = (InternalNode) obj;
        return coordinator == o.coordinator &&
                Objects.equals(nodeIdentifier, o.nodeIdentifier) &&
                Objects.equals(internalUri, o.internalUri) &&
                Objects.equals(nodeVersion, o.nodeVersion) &&
                Objects.equals(nodeGroups, o.nodeGroups);
    }

    public long longHashCode()
    {
        return longHashCode;
    }

    @Override
    public int hashCode()
    {
        return (int) longHashCode;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("internalUri", internalUri)
                .add("nodeVersion", nodeVersion)
                .add("coordinator", coordinator)
                .add("nodeGroups", nodeGroups)
                .toString();
    }
}
