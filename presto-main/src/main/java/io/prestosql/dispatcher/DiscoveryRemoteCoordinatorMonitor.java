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
package io.prestosql.dispatcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.AllNodes;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.Node;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DiscoveryRemoteCoordinatorMonitor
        implements RemoteCoordinatorMonitor
{
    private final InternalNodeManager internalNodeManager;

    private final ExecutorService executorService;
    private final Consumer<AllNodes> updateAllNodesListener = this::updateAllNodes;

    @GuardedBy("this")
    private Set<Node> coordinators = ImmutableSet.of();

    @GuardedBy("this")
    private final List<Consumer<Set<Node>>> listeners = new ArrayList<>();

    @Inject
    public DiscoveryRemoteCoordinatorMonitor(InternalNodeManager internalNodeManager)
    {
        this.internalNodeManager = internalNodeManager;
        this.executorService = newCachedThreadPool(threadsNamed("node-state-events-%s"));
    }

    @PostConstruct
    public void start()
    {
        internalNodeManager.addNodeChangeListener(updateAllNodesListener);
    }

    @PreDestroy
    public void destroy()
    {
        internalNodeManager.removeNodeChangeListener(updateAllNodesListener);
        executorService.shutdownNow();
    }

    @Override
    public synchronized void addCoordinatorsChangeListener(Consumer<Set<Node>> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        Set<Node> coordinators = ImmutableSet.copyOf(this.coordinators);
        executorService.submit(() -> listener.accept(coordinators));
    }

    @Override
    public synchronized void removeCoordinatorsChangeListener(Consumer<Set<Node>> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }

    private synchronized void updateAllNodes(AllNodes allNodes)
    {
        Set<Node> coordinators = allNodes.getActiveCoordinators();
        if (coordinators.equals(this.coordinators)) {
            return;
        }

        this.coordinators = coordinators;
        // notify listeners
        List<Consumer<Set<Node>>> listeners1 = ImmutableList.copyOf(this.listeners);
        try {
            executorService.submit(() -> listeners1.forEach(listener -> listener.accept(coordinators)));
        }
        catch (RejectedExecutionException ignored) {
            // monitor has been destroyed
        }
    }
}
