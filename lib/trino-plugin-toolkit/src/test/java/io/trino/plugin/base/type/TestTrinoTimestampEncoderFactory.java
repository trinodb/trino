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
package io.trino.plugin.base.type;

import io.trino.spi.type.LongTimestamp;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoTimestampEncoderFactory
{
    @Test
    void testShortTimestamp()
    {
        TrinoTimestampEncoder<?> encoder = createTimestampEncoder(TIMESTAMP_MILLIS, ZoneId.of("America/New_York"));
        assertThat(encoder.getTimestamp(new DecodedTimestamp(0, 123_456_789))).isEqualTo(-17_999_877_000L);
        assertThat(encoder.getTimestamp(new DecodedTimestamp(-1, 999_999_999))).isEqualTo(-18_000_000_000L);
    }

    @Test
    void testLongTimestamp()
    {
        TrinoTimestampEncoder<?> encoder = createTimestampEncoder(TIMESTAMP_NANOS, ZoneId.of("America/New_York"));
        assertThat(encoder.getTimestamp(new DecodedTimestamp(0, 123_456_789))).isEqualTo(new LongTimestamp(-17_999_876_544L, 789_000));
        assertThat(encoder.getTimestamp(new DecodedTimestamp(-1, 999_999_999))).isEqualTo(new LongTimestamp(-18_000_000_001L, 999_000));
    }
}
