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
package io.trino.plugin.base.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.spi.metrics.Metrics;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIntList
{
    private final JsonCodec<IntList> codec = jsonCodec(IntList.class);
    private final JsonCodec<Metrics> metricsCodec = jsonCodec(Metrics.class);

    @Test
    void testRoundTrip()
    {
        IntList expected = new IntList(ImmutableList.of(1));

        String json = codec.toJson(expected);
        IntList actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testRoundTripViaMetrics()
    {
        Metrics metrics = new Metrics(ImmutableMap.of("projected_fields", new IntList(ImmutableList.of(1, 2, 3))));

        String json = metricsCodec.toJson(metrics);
        Metrics actual = metricsCodec.fromJson(json);

        assertThat(actual).isEqualTo(metrics);
    }
}
