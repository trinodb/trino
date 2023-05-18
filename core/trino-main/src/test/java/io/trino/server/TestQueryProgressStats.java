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
package io.trino.server;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryProgressStats
{
    @Test
    public void testJson()
    {
        QueryProgressStats expected = new QueryProgressStats(
                123456,
                1111,
                22222,
                3333,
                100000,
                34230492,
                1000,
                100000,
                false,
                OptionalDouble.of(33.33));
        JsonCodec<QueryProgressStats> codec = JsonCodec.jsonCodec(QueryProgressStats.class);

        String json = codec.toJson(expected);
        QueryProgressStats actual = codec.fromJson(json);

        assertThat(actual.getElapsedTimeMillis()).isEqualTo(123456);
        assertThat(actual.getQueuedTimeMillis()).isEqualTo(1111);
        assertThat(actual.getCpuTimeMillis()).isEqualTo(22222);
        assertThat(actual.getScheduledTimeMillis()).isEqualTo(3333);
        assertThat(actual.getCurrentMemoryBytes()).isEqualTo(100000);
        assertThat(actual.getPeakMemoryBytes()).isEqualTo(34230492);
        assertThat(actual.getInputRows()).isEqualTo(1000);
        assertThat(actual.getInputBytes()).isEqualTo(100000);
        assertThat(actual.isBlocked()).isFalse();
        assertThat(actual.getProgressPercentage()).isEqualTo(OptionalDouble.of(33.33));
    }
}
