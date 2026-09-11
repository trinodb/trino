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

import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.trino.plugin.redis.RedisClusterTopology.parseClusterSlots;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRedisClusterNodeDiscovery
{
    @Test
    void testParseClusterSlotsThreeMasters()
    {
        // Simulates CLUSTER SLOTS RESP response for a 3-master cluster:
        // [startSlot, endSlot, [ip, port, id], [replicaIp, replicaPort, replicaId], ...]
        List<Object> response = List.of(
                List.of(0L, 5460L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30001L, "master-id-1".getBytes(StandardCharsets.UTF_8)), List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30004L, "replica-id-1".getBytes(StandardCharsets.UTF_8))),
                List.of(5461L, 10922L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30002L, "master-id-2".getBytes(StandardCharsets.UTF_8)), List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30005L, "replica-id-2".getBytes(StandardCharsets.UTF_8))),
                List.of(10923L, 16383L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30003L, "master-id-3".getBytes(StandardCharsets.UTF_8)), List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30006L, "replica-id-3".getBytes(StandardCharsets.UTF_8))));

        RedisClusterTopology topology = parseClusterSlots(response);

        assertThat(topology.isComplete()).isTrue();
        assertThat(topology.getPrimaries()).containsExactlyInAnyOrder(
                HostAddress.fromParts("127.0.0.1", 30001),
                HostAddress.fromParts("127.0.0.1", 30002),
                HostAddress.fromParts("127.0.0.1", 30003));
    }

    @Test
    void testSlotRouting()
    {
        List<Object> response = List.of(
                List.of(0L, 5460L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30001L, "id-1".getBytes(StandardCharsets.UTF_8))),
                List.of(5461L, 10922L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30002L, "id-2".getBytes(StandardCharsets.UTF_8))),
                List.of(10923L, 16383L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30003L, "id-3".getBytes(StandardCharsets.UTF_8))));

        RedisClusterTopology topology = parseClusterSlots(response);

        // Slot 0 should route to primary at 30001
        assertThat(topology.getPrimaryForSlot(0)).isEqualTo(HostAddress.fromParts("127.0.0.1", 30001));
        // Slot 5461 should route to primary at 30002
        assertThat(topology.getPrimaryForSlot(5461)).isEqualTo(HostAddress.fromParts("127.0.0.1", 30002));
        // Slot 10923 should route to primary at 30003
        assertThat(topology.getPrimaryForSlot(10923)).isEqualTo(HostAddress.fromParts("127.0.0.1", 30003));
    }

    @Test
    void testIncompleteTopology()
    {
        // Only covers slots 0-8191, missing 8192-16383
        List<Object> response = List.of(
                List.of(0L, 8191L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 30001L, "id-1".getBytes(StandardCharsets.UTF_8))));

        RedisClusterTopology topology = parseClusterSlots(response);

        assertThat(topology.isComplete()).isFalse();
        assertThat(topology.getPrimaries()).containsExactly(HostAddress.fromParts("127.0.0.1", 30001));
        // Accessing an unassigned slot should fail
        assertThatThrownBy(() -> topology.getPrimaryForSlot(8192))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no primary assigned for slot");
    }

    @Test
    void testEmptyResponse()
    {
        List<Object> response = List.of();

        RedisClusterTopology topology = parseClusterSlots(response);

        assertThat(topology.isComplete()).isFalse();
        assertThat(topology.getPrimaries()).isEmpty();
    }

    @Test
    void testSinglePrimaryOwnsAllSlots()
    {
        List<Object> response = List.of(
                List.of(0L, 16383L, List.of("127.0.0.1".getBytes(StandardCharsets.UTF_8), 6379L, "single-master".getBytes(StandardCharsets.UTF_8))));

        RedisClusterTopology topology = parseClusterSlots(response);

        assertThat(topology.isComplete()).isTrue();
        assertThat(topology.getPrimaries()).containsExactly(HostAddress.fromParts("127.0.0.1", 6379));
        // Every slot should route to the single primary
        for (int slot = 0; slot < RedisClusterTopology.TOTAL_SLOTS; slot += 1000) {
            assertThat(topology.getPrimaryForSlot(slot)).isEqualTo(HostAddress.fromParts("127.0.0.1", 6379));
        }
    }
}
