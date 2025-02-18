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
package io.trino.execution;

import io.airlift.stats.TDigest;
import io.trino.plugin.base.metrics.TDigestHistogram;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestDistributionSnapshot
{
    @Test
    void testConvertFromTDigestHistogram()
    {
        TDigest digest = new TDigest();
        digest.add(1.0);
        digest.add(10.0);
        digest.add(10.0);
        digest.add(1.0);
        digest.add(4.0);
        digest.add(5.0);
        digest.add(1.0);
        digest.add(3.0);
        digest.add(7.0);

        TDigestHistogram histogram = new TDigestHistogram(digest);
        DistributionSnapshot snapshot = new DistributionSnapshot(histogram);

        assertThat(snapshot.total()).isEqualTo(9);
        assertThat(snapshot.min()).isEqualTo(1.0);
        assertThat(snapshot.max()).isEqualTo(10.0);
        assertThat(snapshot.p01()).isEqualTo(1.0);
        assertThat(snapshot.p05()).isEqualTo(1.0);
        assertThat(snapshot.p10()).isEqualTo(1.0);
        assertThat(snapshot.p25()).isEqualTo(1.0);
        assertThat(snapshot.p50()).isEqualTo(4.0);
        assertThat(snapshot.p75()).isEqualTo(7.0);
        assertThat(snapshot.p90()).isEqualTo(10.0);
        assertThat(snapshot.p95()).isEqualTo(10.0);
        assertThat(snapshot.p99()).isEqualTo(10.0);
    }
}
