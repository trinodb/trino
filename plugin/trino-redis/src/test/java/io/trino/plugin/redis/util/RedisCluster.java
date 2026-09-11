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
package io.trino.plugin.redis.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.redis.RedisClusterTopology;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.SslOptions;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkState;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/**
 * Manages a real multi-primary Redis Cluster using a single Docker container
 * running multiple Redis instances on different ports.
 * <p>
 * Starts {@code numPrimaries} Redis instances with cluster mode enabled within
 * one container, forms them into a cluster by distributing hash slots evenly,
 * and provides seed addresses and a {@link RedisClusterClient} for cluster-aware
 * data loading.
 * <p>
 * Using a single container avoids cross-container networking issues: all Redis
 * instances share the same network namespace and can reach each other via
 * {@code 127.0.0.1}. Fixed port bindings (7000, 7001, ...) make the advertised
 * addresses reachable from the test JVM as well.
 */
public class RedisCluster
        implements Closeable
{
    private static final Logger log = Logger.get(RedisCluster.class);

    private static final int DEFAULT_NUM_PRIMARIES = 3;
    private static final int DEFAULT_NUM_REPLICAS = 0;
    private static final int DEFAULT_BASE_PORT = 7000;
    private static final int CLUSTER_TIMEOUT_MILLIS = 5000;
    private static final String CONTAINER_CERTS_DIR = "/etc/redis/certs/";

    // RedisCluster uses fixed host port bindings, so only one cluster can be active at a time.
    private static final Semaphore CLUSTER_SEMAPHORE = new Semaphore(1);

    private GenericContainer<?> container;
    private final List<RedisClient> clients;
    private final List<RedisClient> replicaClients;
    private final List<HostAndPort> jedisSeedAddresses;
    private final List<com.google.common.net.HostAndPort> seedAddresses;
    private RedisClusterClient redisClusterClient;
    private final boolean tls;
    private final boolean auth;

    public RedisCluster()
    {
        this(DEFAULT_NUM_PRIMARIES, DEFAULT_BASE_PORT, DEFAULT_NUM_REPLICAS, false, false);
    }

    public RedisCluster(int numPrimaries, int basePort)
    {
        this(numPrimaries, basePort, DEFAULT_NUM_REPLICAS, false, false);
    }

    public RedisCluster(int numPrimaries, int basePort, int numReplicas)
    {
        this(numPrimaries, basePort, numReplicas, false, false);
    }

    public RedisCluster(int numPrimaries, int basePort, int numReplicas, boolean tls, boolean auth)
    {
        this.tls = tls;
        this.auth = auth;
        clients = new ArrayList<>(numPrimaries);
        replicaClients = new ArrayList<>(numReplicas);
        jedisSeedAddresses = new ArrayList<>(numPrimaries);
        seedAddresses = new ArrayList<>(numPrimaries);

        // RedisCluster binds fixed host ports; serialize so concurrent tests do not conflict.
        CLUSTER_SEMAPHORE.acquireUninterruptibly();
        boolean acquired = true;

        try {
            startCluster(numPrimaries, basePort, numReplicas);
            acquired = false;
        }
        finally {
            if (acquired) {
                CLUSTER_SEMAPHORE.release();
            }
        }
    }

    private void startCluster(int numPrimaries, int basePort, int numReplicas)
    {
        // Build the command to start all Redis instances in a single container.
        // Each instance gets its own data directory to avoid AOF/cluster-config file conflicts.
        List<Integer> primaryPorts = new ArrayList<>(numPrimaries);
        for (int i = 0; i < numPrimaries; i++) {
            primaryPorts.add(basePort + i);
        }

        List<Integer> replicaPorts = new ArrayList<>(numReplicas);
        for (int i = 0; i < numReplicas; i++) {
            replicaPorts.add(basePort + numPrimaries + i);
        }

        List<Integer> allPorts = new ArrayList<>(primaryPorts);
        allPorts.addAll(replicaPorts);

        StringBuilder command = new StringBuilder();
        command.append("mkdir -p");
        for (int i = 0; i < allPorts.size(); i++) {
            command.append(" /data/").append(i);
        }
        command.append("; ");

        for (int i = 0; i < allPorts.size(); i++) {
            int port = allPorts.get(i);
            command.append("redis-server");
            if (tls) {
                // Use a plaintext port for internal cluster bus communication and
                // a separate TLS port for client connections.  --cluster-announce-port
                // makes CLUSTER SLOTS return the TLS port so clients connect via TLS.
                command.append(" --port ").append(port)
                        .append(" --tls-port ").append(port + 1000)
                        .append(" --cluster-announce-port ").append(port + 1000)
                        .append(" --cluster-announce-bus-port ").append(port + 10000)
                        .append(" --tls-cert-file ").append(CONTAINER_CERTS_DIR).append("redis.crt")
                        .append(" --tls-key-file ").append(CONTAINER_CERTS_DIR).append("redis.key")
                        .append(" --tls-ca-cert-file ").append(CONTAINER_CERTS_DIR).append("ca.crt");
            }
            else {
                command.append(" --port ").append(port);
            }
            command.append(" --cluster-enabled yes")
                    .append(" --cluster-config-file nodes.conf")
                    .append(" --cluster-node-timeout ").append(CLUSTER_TIMEOUT_MILLIS)
                    .append(" --dir /data/").append(i)
                    .append(" --appendonly no");
            if (auth) {
                command.append(" --requirepass ").append(RedisServer.PASSWORD);
                command.append(" --masterauth ").append(RedisServer.PASSWORD);
            }
            command.append(" & ");
        }
        command.append("wait");

        // Expose client ports (TLS ports for TLS mode, plain ports otherwise) with fixed bindings
        // so cluster-announce-port is reachable from the test JVM.
        List<Integer> clientPorts = new ArrayList<>(allPorts.size());
        for (int port : allPorts) {
            clientPorts.add(tls ? port + 1000 : port);
        }
        ImmutableList.Builder<String> portBindings = ImmutableList.builder();
        for (int clientPort : clientPorts) {
            portBindings.add(clientPort + ":" + clientPort);
        }

        container = new GenericContainer<>("redis:" + RedisServer.LATEST_VERSION)
                .withExposedPorts(clientPorts.toArray(new Integer[0]))
                .withCommand("/bin/sh", "-c", command.toString());
        if (tls) {
            container.withCopyFileToContainer(forClasspathResource("tls/ca.crt", 0644), CONTAINER_CERTS_DIR + "ca.crt")
                    .withCopyFileToContainer(forClasspathResource("tls/redis.crt", 0644), CONTAINER_CERTS_DIR + "redis.crt")
                    .withCopyFileToContainer(forClasspathResource("tls/redis.key", 0644), CONTAINER_CERTS_DIR + "redis.key");
        }
        container.setPortBindings(portBindings.build());
        container.start();

        // Create clients for each Redis instance and set cluster-announce-ip/port
        String announceIp = "127.0.0.1";
        for (int i = 0; i < numPrimaries; i++) {
            int port = primaryPorts.get(i);
            int clientPort = tls ? port + 1000 : port;
            RedisClient client = RedisClient.builder()
                    .hostAndPort(announceIp, clientPort)
                    .clientConfig(buildClientConfig())
                    .build();
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-ip", announceIp);
                connection.getStatusCodeReply();
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-port", Integer.toString(clientPort));
                connection.getStatusCodeReply();
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-bus-port", Integer.toString(port + 10000));
                connection.getStatusCodeReply();
            }
            clients.add(client);
            jedisSeedAddresses.add(new HostAndPort(announceIp, clientPort));
            seedAddresses.add(com.google.common.net.HostAndPort.fromParts(announceIp, clientPort));
        }

        for (int i = 0; i < numReplicas; i++) {
            int port = replicaPorts.get(i);
            int clientPort = tls ? port + 1000 : port;
            RedisClient client = RedisClient.builder()
                    .hostAndPort(announceIp, clientPort)
                    .clientConfig(buildClientConfig())
                    .build();
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-ip", announceIp);
                connection.getStatusCodeReply();
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-port", Integer.toString(clientPort));
                connection.getStatusCodeReply();
                connection.sendCommand(Protocol.Command.CONFIG, "SET", "cluster-announce-bus-port", Integer.toString(port + 10000));
                connection.getStatusCodeReply();
            }
            replicaClients.add(client);
        }

        // Form the cluster: MEET all nodes, distribute slots, and attach replicas
        formCluster(primaryPorts, replicaPorts);

        // Create RedisClusterClient for cluster-aware data loading
        redisClusterClient = RedisClusterClient.builder()
                .nodes(ImmutableSet.copyOf(jedisSeedAddresses))
                .clientConfig(buildClientConfig())
                .build();
    }

    private void formCluster(List<Integer> primaryPorts, List<Integer> replicaPorts)
    {
        int numPrimaries = primaryPorts.size();

        // Meet all primary nodes from the first primary
        RedisClient firstClient = clients.get(0);
        for (int i = 1; i < numPrimaries; i++) {
            try (Connection connection = firstClient.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "MEET", "127.0.0.1", Integer.toString(primaryPorts.get(i)));
                connection.getStatusCodeReply();
            }
        }

        // Meet replica nodes from the first primary so they join the cluster
        for (int port : replicaPorts) {
            try (Connection connection = firstClient.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "MEET", "127.0.0.1", Integer.toString(port));
                connection.getStatusCodeReply();
            }
        }

        // Distribute slots evenly across primaries
        int slotsPerNode = RedisClusterTopology.TOTAL_SLOTS / numPrimaries;
        int remainder = RedisClusterTopology.TOTAL_SLOTS % numPrimaries;
        int currentSlot = 0;
        for (int i = 0; i < numPrimaries; i++) {
            int slotsForThisNode = slotsPerNode + (i < remainder ? 1 : 0);
            if (slotsForThisNode > 0) {
                int endSlot = currentSlot + slotsForThisNode - 1;
                try (Connection connection = clients.get(i).getPool().getResource()) {
                    connection.sendCommand(
                            Protocol.Command.CLUSTER,
                            "ADDSLOTSRANGE",
                            Integer.toString(currentSlot),
                            Integer.toString(endSlot));
                    connection.getStatusCodeReply();
                }
                currentSlot = endSlot + 1;
            }
        }

        // Wait for cluster to be ready
        waitForClusterReady();

        // Verify all slots are assigned
        verifyClusterState();

        // Attach replicas to primaries if any were requested
        if (!replicaClients.isEmpty()) {
            assignReplicas();
        }
    }

    private void waitForClusterReady()
    {
        long deadlineMillis = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadlineMillis) {
            boolean allReady = true;
            for (RedisClient client : clients) {
                String info;
                try (Connection connection = client.getPool().getResource()) {
                    connection.sendCommand(Protocol.Command.CLUSTER, "INFO");
                    info = SafeEncoder.encode((byte[]) connection.getOne());
                }
                catch (Exception e) {
                    allReady = false;
                    break;
                }
                if (info == null || !info.contains("cluster_state:ok")) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) {
                return;
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Redis cluster to be ready", e);
            }
        }
        throw new IllegalStateException("Redis cluster did not become ready within 60 seconds");
    }

    private void verifyClusterState()
    {
        try (Connection connection = clients.get(0).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
            Object response = connection.getOne();
            checkState(response instanceof List, "CLUSTER SLOTS returned unexpected type: %s", response.getClass());
            @SuppressWarnings("unchecked")
            List<Object> slotRanges = (List<Object>) response;
            int assignedSlots = 0;
            for (Object slotRangeObj : slotRanges) {
                @SuppressWarnings("unchecked")
                List<Object> slotRange = (List<Object>) slotRangeObj;
                int startSlot = ((Long) slotRange.get(0)).intValue();
                int endSlot = ((Long) slotRange.get(1)).intValue();
                assignedSlots += (endSlot - startSlot + 1);
            }
            checkState(assignedSlots == RedisClusterTopology.TOTAL_SLOTS,
                    "Cluster has %s assigned slots, expected %s",
                    assignedSlots,
                    RedisClusterTopology.TOTAL_SLOTS);
        }
    }

    /**
     * Returns the seed addresses for the cluster, suitable for use as {@code redis.nodes}.
     */
    public List<com.google.common.net.HostAndPort> getSeedAddresses()
    {
        return ImmutableList.copyOf(seedAddresses);
    }

    /**
     * Returns a comma-separated list of seed addresses for the {@code redis.nodes} property.
     */
    public String getSeedAddressesString()
    {
        return seedAddresses.stream()
                .map(com.google.common.net.HostAndPort::toString)
                .reduce((a, b) -> a + "," + b)
                .orElseThrow();
    }

    /**
     * Returns a {@link RedisClusterClient} for cluster-aware data loading.
     */
    public RedisClusterClient getRedisClusterClient()
    {
        return redisClusterClient;
    }

    /**
     * Returns a {@link RedisClient} connected to the first primary, for direct operations.
     */
    public RedisClient getClient()
    {
        return clients.get(0);
    }

    /**
     * Returns a {@link RedisClient} connected to the primary at the given index.
     */
    public RedisClient getClient(int index)
    {
        return clients.get(index);
    }

    /**
     * Returns the client port of the primary at the given index.
     */
    public int getPort(int index)
    {
        return jedisSeedAddresses.get(index).getPort();
    }

    /**
     * Returns the internal (plaintext) port of the primary at the given index,
     * used for MIGRATE commands which connect inside the container without TLS.
     */
    public int getInternalPort(int index)
    {
        int clientPort = jedisSeedAddresses.get(index).getPort();
        return tls ? clientPort - 1000 : clientPort;
    }

    public int getKeySlot(String key)
    {
        try (Connection connection = clients.get(0).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "KEYSLOT", key);
            Object reply = connection.getOne();
            checkState(reply != null, "CLUSTER KEYSLOT returned null for %s", key);
            return ((Long) reply).intValue();
        }
    }

    public int getPrimaryIndexForSlot(int slot)
    {
        try (Connection connection = clients.get(0).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SLOTS");
            Object response = connection.getOne();
            checkState(response instanceof List, "CLUSTER SLOTS returned unexpected type: %s", response.getClass());
            @SuppressWarnings("unchecked")
            List<Object> slotRanges = (List<Object>) response;
            for (Object slotRangeObj : slotRanges) {
                @SuppressWarnings("unchecked")
                List<Object> slotRange = (List<Object>) slotRangeObj;
                int startSlot = ((Long) slotRange.get(0)).intValue();
                int endSlot = ((Long) slotRange.get(1)).intValue();
                if (slot >= startSlot && slot <= endSlot) {
                    @SuppressWarnings("unchecked")
                    List<Object> masterInfo = (List<Object>) slotRange.get(2);
                    int masterPort = ((Long) masterInfo.get(1)).intValue();
                    for (int i = 0; i < clients.size(); i++) {
                        if (jedisSeedAddresses.get(i).getPort() == masterPort) {
                            return i;
                        }
                    }
                    throw new IllegalStateException("No primary found for port " + masterPort);
                }
            }
        }
        throw new IllegalStateException("No primary found for slot " + slot);
    }

    public void migrateSlotAndKey(String key, int sourceIndex, int targetIndex)
    {
        int slot = getKeySlot(key);
        String sourceNodeId = getNodeId(sourceIndex);
        String targetNodeId = getNodeId(targetIndex);
        int targetPort = getInternalPort(targetIndex);

        // Mark slot as migrating on source and importing on target
        try (Connection connection = clients.get(sourceIndex).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "MIGRATING", targetNodeId);
            connection.getStatusCodeReply();
        }
        try (Connection connection = clients.get(targetIndex).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "IMPORTING", sourceNodeId);
            connection.getStatusCodeReply();
        }

        // MIGRATE KEYS atomically moves the key (handles DUMP/RESTORE/DEL/TTL server-side)
        try (Connection connection = clients.get(sourceIndex).getPool().getResource()) {
            connection.sendCommand(
                    Protocol.Command.MIGRATE,
                    "127.0.0.1",
                    Integer.toString(targetPort),
                    "",
                    "0",
                    "5000",
                    "REPLACE",
                    "KEYS",
                    key);
            connection.getStatusCodeReply();
        }

        // Finalize ownership on all primaries so clients get MOVED from source to target
        for (RedisClient client : clients) {
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "NODE", targetNodeId);
                connection.getStatusCodeReply();
            }
        }

        waitForClusterReady();
    }

    /**
     * Finalizes a slot migration by assigning slot ownership to the target
     * without moving any keys.  Use after {@link #prepareAskingSlot} to restore
     * a stable cluster state — the key has already been moved.
     */
    public void finalizeSlotMigration(String key, int targetIndex)
    {
        int slot = getKeySlot(key);
        String targetNodeId = getNodeId(targetIndex);
        for (RedisClient client : clients) {
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "NODE", targetNodeId);
                connection.getStatusCodeReply();
            }
        }
        waitForClusterReady();
    }

    public void migrateSlot(int slot, int targetIndex)
    {
        int sourceIndex = getPrimaryIndexForSlot(slot);
        String sourceNodeId = getNodeId(sourceIndex);
        String targetNodeId = getNodeId(targetIndex);
        RedisClient sourceClient = clients.get(sourceIndex);
        RedisClient targetClient = clients.get(targetIndex);
        int targetPort = getInternalPort(targetIndex);

        try (Connection connection = sourceClient.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "MIGRATING", targetNodeId);
            connection.getStatusCodeReply();
        }
        try (Connection connection = targetClient.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "IMPORTING", sourceNodeId);
            connection.getStatusCodeReply();
        }

        // Move all keys in the slot using MIGRATE (handles ASKING/RESTORE/DEL/TTL).
        // CLUSTER GETKEYSINSLOT has no cursor, so we delete each batch and call
        // again until the slot is empty.
        while (true) {
            List<String> keys = getKeysInSlot(sourceClient, slot);
            if (keys.isEmpty()) {
                break;
            }
            List<String> migrateArgs = new ArrayList<>();
            migrateArgs.add("127.0.0.1");
            migrateArgs.add(Integer.toString(targetPort));
            migrateArgs.add("");
            migrateArgs.add("0");
            migrateArgs.add("5000");
            migrateArgs.add("REPLACE");
            migrateArgs.add("KEYS");
            migrateArgs.addAll(keys);
            try (Connection connection = sourceClient.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.MIGRATE, migrateArgs.toArray(new String[0]));
                connection.getStatusCodeReply();
            }
        }

        // Finalize ownership on all primaries
        for (RedisClient client : clients) {
            try (Connection connection = client.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "NODE", targetNodeId);
                connection.getStatusCodeReply();
            }
        }

        waitForClusterReady();
    }

    private List<String> getKeysInSlot(RedisClient client, int slot)
    {
        try (Connection connection = client.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "GETKEYSINSLOT", Integer.toString(slot), "1000");
            Object reply = connection.getOne();
            if (reply == null) {
                return List.of();
            }
            checkState(reply instanceof List, "CLUSTER GETKEYSINSLOT returned unexpected type: %s", reply.getClass());
            @SuppressWarnings("unchecked")
            List<Object> entries = (List<Object>) reply;
            if (entries.isEmpty()) {
                return List.of();
            }
            List<String> keys = new ArrayList<>(entries.size());
            for (Object entry : entries) {
                keys.add(SafeEncoder.encode((byte[]) entry));
            }
            return keys;
        }
    }

    public void prepareAskingSlot(String key, int sourceIndex, int targetIndex)
    {
        int slot = getKeySlot(key);
        String sourceNodeId = getNodeId(sourceIndex);
        String targetNodeId = getNodeId(targetIndex);
        int targetPort = getInternalPort(targetIndex);

        // Mark slot as migrating/importing, move key, but do not set NODE owner
        try (Connection connection = clients.get(sourceIndex).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "MIGRATING", targetNodeId);
            connection.getStatusCodeReply();
        }
        try (Connection connection = clients.get(targetIndex).getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "SETSLOT", Integer.toString(slot), "IMPORTING", sourceNodeId);
            connection.getStatusCodeReply();
        }

        // MIGRATE KEYS atomically moves the key (handles DUMP/RESTORE/DEL/TTL server-side)
        try (Connection connection = clients.get(sourceIndex).getPool().getResource()) {
            connection.sendCommand(
                    Protocol.Command.MIGRATE,
                    "127.0.0.1",
                    Integer.toString(targetPort),
                    "",
                    "0",
                    "5000",
                    "REPLACE",
                    "KEYS",
                    key);
            connection.getStatusCodeReply();
        }

        // Slot remains in MIGRATING/IMPORTING state; source returns ASK target.
        // cluster_state is fail because the slot has no owner, so we wait briefly
        // for the cluster bus to propagate the importing/migrating state.
        try {
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for ASK slot state", e);
        }
    }

    private void assignReplicas()
    {
        for (int i = 0; i < replicaClients.size(); i++) {
            int primaryIndex = i % clients.size();
            String primaryNodeId = getNodeId(clients.get(primaryIndex));
            try (Connection connection = replicaClients.get(i).getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "REPLICATE", primaryNodeId);
                connection.getStatusCodeReply();
            }
        }

        long deadlineMillis = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadlineMillis) {
            boolean allConnected = true;
            for (int i = 0; i < replicaClients.size(); i++) {
                int primaryIndex = i % clients.size();
                String primaryNodeId = getNodeId(clients.get(primaryIndex));
                String expectedLine = "slave " + primaryNodeId;
                try (Connection connection = replicaClients.get(i).getPool().getResource()) {
                    connection.sendCommand(Protocol.Command.CLUSTER, "NODES");
                    String info = SafeEncoder.encode((byte[]) connection.getOne());
                    if (!info.contains("myself,slave") || !info.contains(expectedLine) || !info.contains("connected")) {
                        allConnected = false;
                        break;
                    }
                }
                catch (Exception e) {
                    allConnected = false;
                    break;
                }
            }
            if (allConnected) {
                return;
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for replicas to connect", e);
            }
        }
        throw new IllegalStateException("Replicas did not become connected within 60 seconds");
    }

    private String getNodeId(int primaryIndex)
    {
        return getNodeId(clients.get(primaryIndex));
    }

    private String getNodeId(RedisClient client)
    {
        try (Connection connection = client.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.CLUSTER, "NODES");
            String info = SafeEncoder.encode((byte[]) connection.getOne());
            for (String line : info.split("\n", -1)) {
                String[] parts = line.trim().split("\\s+", -1);
                if (parts.length >= 3 && parts[2].contains("myself")) {
                    return parts[0];
                }
            }
            throw new IllegalStateException("Could not find node id in CLUSTER NODES output");
        }
    }

    /**
     * Stops the primary at the given index and waits for its replica to be promoted.
     * Returns the index of the new primary (the replica's former index).
     */
    public int killPrimaryAndWaitForFailover(int primaryIndex)
    {
        checkState(!replicaClients.isEmpty(), "No replicas configured, cannot fail over");
        checkState(primaryIndex < clients.size() && primaryIndex < replicaClients.size(),
                "Primary %s has no configured replica",
                primaryIndex);

        RedisClient primary = clients.get(primaryIndex);
        RedisClient replica = replicaClients.get(primaryIndex);

        // Shut down the primary so the cluster marks it as fail
        try (Connection connection = primary.getPool().getResource()) {
            connection.sendCommand(Protocol.Command.SHUTDOWN, "NOSAVE");
        }
        catch (Exception e) {
            // SHUTDOWN closes the connection, so an exception is expected
            log.info("Sent SHUTDOWN to primary %s", primaryIndex);
        }

        primary.close();

        // Promoted replica is now a primary; keep a stable client for it
        clients.set(primaryIndex, replica);

        // Wait for the replica to be promoted to master
        String replicaNodeId = getNodeId(replica);
        long deadlineMillis = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadlineMillis) {
            try (Connection connection = replica.getPool().getResource()) {
                connection.sendCommand(Protocol.Command.CLUSTER, "NODES");
                String info = SafeEncoder.encode((byte[]) connection.getOne());
                if (info.contains(replicaNodeId) && info.contains("myself,master") && info.contains("connected")) {
                    break;
                }
            }
            catch (Exception e) {
                // replica may still be starting up; keep waiting
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for failover", e);
            }
        }

        waitForClusterReady();

        return primaryIndex;
    }

    private DefaultJedisClientConfig buildClientConfig()
    {
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();
        if (tls) {
            builder.sslOptions(buildSslOptions());
        }
        if (auth) {
            builder.password(RedisServer.PASSWORD);
        }
        return builder.build();
    }

    private static SslOptions buildSslOptions()
    {
        return SslOptions.builder()
                .keystore(new File(RedisServer.getKeystorePath()), RedisServer.TLS_STORE_PASSWORD.toCharArray())
                .truststore(new File(RedisServer.getTruststorePath()), RedisServer.TLS_STORE_PASSWORD.toCharArray())
                .build();
    }

    @Override
    public void close()
    {
        try {
            redisClusterClient.close();
        }
        catch (Exception e) {
            // ignore
        }
        for (RedisClient client : clients) {
            try {
                client.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
        for (RedisClient client : replicaClients) {
            try {
                client.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
        try {
            container.close();
        }
        catch (Exception e) {
            // ignore
        }
        finally {
            CLUSTER_SEMAPHORE.release();
        }
    }
}
