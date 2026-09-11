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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Represents the slot-to-primary routing topology of a Redis Cluster, discovered
 * via the {@code CLUSTER SLOTS} command.  Maps all 16,384 Redis hash slots to
 * their owning primary node and provides key-based routing.
 * <p>
 * The topology is an immutable snapshot; call {@link #discover} to build a new
 * instance from the live cluster.
 */
public final class RedisClusterTopology
{
    private static final Logger log = Logger.get(RedisClusterTopology.class);

    public static final int TOTAL_SLOTS = 16384;

    private final HostAddress[] slotToPrimary;
    private final Set<HostAddress> primaries;

    private RedisClusterTopology(HostAddress[] slotToPrimary, Set<HostAddress> primaries)
    {
        this.slotToPrimary = slotToPrimary;
        this.primaries = ImmutableSet.copyOf(primaries);
    }

    /**
     * Returns the primary node that owns the slot for the given key.
     */
    public HostAddress getPrimaryForKey(String key)
    {
        return getPrimaryForSlot(JedisClusterCRC16.getSlot(key));
    }

    /**
     * Returns the primary node that owns the given slot.
     */
    public HostAddress getPrimaryForSlot(int slot)
    {
        checkArgument(slot >= 0 && slot < TOTAL_SLOTS, "slot out of range: %s", slot);
        HostAddress primary = slotToPrimary[slot];
        checkArgument(primary != null, "no primary assigned for slot %s; topology may be incomplete", slot);
        return primary;
    }

    /**
     * Returns all primary nodes in this topology.
     */
    public Set<HostAddress> getPrimaries()
    {
        return primaries;
    }

    /**
     * Returns true if every slot 0..16383 is assigned to a primary.
     */
    public boolean isComplete()
    {
        for (HostAddress primary : slotToPrimary) {
            if (primary == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Discovers the cluster topology from one of the given seed nodes using
     * the {@code CLUSTER SLOTS} command.  Each seed is tried in turn until one
     * responds with a complete topology.
     *
     * @throws IllegalStateException if no seed can provide a complete topology
     */
    public static RedisClusterTopology discover(Set<HostAddress> seedNodes, DefaultJedisClientConfig clientConfig)
    {
        requireNonNull(seedNodes, "seedNodes is null");
        requireNonNull(clientConfig, "clientConfig is null");
        checkArgument(!seedNodes.isEmpty(), "seedNodes is empty");

        List<Exception> failures = new ArrayList<>();
        for (HostAddress seed : seedNodes) {
            try (Connection connection = new Connection(
                    new HostAndPort(seed.getHostText(), seed.getPort()),
                    clientConfig)) {
                connection.sendCommand(new CommandArguments(Protocol.Command.CLUSTER).add("SLOTS"));
                Object response = connection.getOne();
                RedisClusterTopology topology = parseClusterSlots(response);
                if (topology.isComplete()) {
                    log.info("Discovered Redis Cluster topology with %d primaries from seed %s", topology.getPrimaries().size(), seed);
                    return topology;
                }
                failures.add(new IllegalStateException("Seed node " + seed + " returned an incomplete slot map"));
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to discover Redis cluster topology from seed %s", seed);
                failures.add(e);
            }
        }
        IllegalStateException exception = new IllegalStateException(
                "Unable to discover a complete Redis Cluster topology from any configured seed node: " + seedNodes);
        failures.forEach(exception::addSuppressed);
        throw exception;
    }

    /**
     * Parses the raw RESP response of {@code CLUSTER SLOTS} into a topology.
     * <p>
     * The response is a list of slot-range entries.  Each entry is:
     * <pre>
     * [startSlot, endSlot, [masterIp, masterPort, masterId], [replicaIp, replicaPort, replicaId], ...]
     * </pre>
     */
    static RedisClusterTopology parseClusterSlots(Object response)
    {
        requireNonNull(response, "response is null");
        checkArgument(response instanceof List, "expected a list for CLUSTER SLOTS response, got %s", response.getClass());

        HostAddress[] slotToPrimary = new HostAddress[TOTAL_SLOTS];
        Set<HostAddress> primaries = new HashSet<>();

        @SuppressWarnings("unchecked")
        List<Object> slotRanges = (List<Object>) response;

        for (Object slotRangeObj : slotRanges) {
            checkArgument(slotRangeObj instanceof List, "expected a list for slot range, got %s", slotRangeObj.getClass());
            @SuppressWarnings("unchecked")
            List<Object> slotRange = (List<Object>) slotRangeObj;

            checkArgument(slotRange.size() >= 3, "slot range must have at least 3 elements, got %s", slotRange.size());

            int startSlot = ((Long) slotRange.get(0)).intValue();
            int endSlot = ((Long) slotRange.get(1)).intValue();

            @SuppressWarnings("unchecked")
            List<Object> masterInfo = (List<Object>) slotRange.get(2);
            checkArgument(masterInfo.size() >= 2, "master info must have at least 2 elements");

            String masterIp = SafeEncoder.encode((byte[]) masterInfo.get(0));
            int masterPort = ((Long) masterInfo.get(1)).intValue();
            HostAddress primary = HostAddress.fromParts(masterIp, masterPort);
            primaries.add(primary);

            for (int slot = startSlot; slot <= endSlot; slot++) {
                checkArgument(slot >= 0 && slot < TOTAL_SLOTS, "slot out of range: %s", slot);
                slotToPrimary[slot] = primary;
            }
        }

        return new RedisClusterTopology(slotToPrimary, primaries);
    }
}
