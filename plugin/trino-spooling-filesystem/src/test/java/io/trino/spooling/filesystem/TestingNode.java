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
package io.trino.spooling.filesystem;

import io.trino.spi.HostAddress;
import io.trino.spi.Node;

import static java.util.Objects.requireNonNull;

class TestingNode
        implements Node
{
    private final String nodeIdentifier;

    public TestingNode(String nodeIdentifier)
    {
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
    }

    @Override
    public String getHost()
    {
        return "localhost";
    }

    @Override
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromParts("localhost", 8080);
    }

    @Override
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String getVersion()
    {
        return "1.0.0";
    }

    @Override
    public boolean isCoordinator()
    {
        return false;
    }
}
