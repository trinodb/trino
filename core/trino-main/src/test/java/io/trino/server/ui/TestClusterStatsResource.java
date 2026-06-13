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
package io.trino.server.ui;

import io.airlift.json.JsonCodec;
import io.trino.server.ui.ClusterStatsResource.ClusterStats;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestClusterStatsResource
{
    private static final JsonCodec<ClusterStats> CODEC = jsonCodec(ClusterStats.class);

    @Test
    public void testTotalCpuTimeSecsPreservesSubSecondPrecision()
    {
        ClusterStats stats = new ClusterStats(1, 0, 2, 1, 4, 10, 16, 1024.0, 1000, 2048, 3.75);

        String json = CODEC.toJson(stats);
        assertThat(json).contains("\"totalCpuTimeSecs\" : 3.75");

        ClusterStats decoded = CODEC.fromJson(json);
        assertThat(decoded.getTotalCpuTimeSecs()).isEqualTo(3.75);
    }
}
