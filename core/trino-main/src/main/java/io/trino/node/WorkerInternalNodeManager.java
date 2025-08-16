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

import io.trino.spi.HostAddress;

import java.util.function.Consumer;

public class WorkerInternalNodeManager
        implements InternalNodeManager
{
    @Override
    public AllNodes getAllNodes()
    {
        throw new UnsupportedOperationException("Nodes cannot be listed on a worker node");
    }

    @Override
    public boolean isGone(HostAddress hostAddress)
    {
        throw new UnsupportedOperationException("Nodes cannot be listed on a worker node");
    }

    @Override
    public boolean refreshNodes(boolean forceAndWait)
    {
        throw new UnsupportedOperationException("Nodes cannot be listed on a worker node");
    }

    @Override
    public void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        throw disabledException();
    }

    @Override
    public void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        throw disabledException();
    }

    private static UnsupportedOperationException disabledException()
    {
        return new UnsupportedOperationException("Nodes cannot be retrieved on a worker node");
    }
}
