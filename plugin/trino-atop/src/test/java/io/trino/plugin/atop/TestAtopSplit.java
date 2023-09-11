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
package io.trino.plugin.atop;

import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAtopSplit
{
    @Test
    public void testSerialization()
    {
        JsonCodec<AtopSplit> codec = JsonCodec.jsonCodec(AtopSplit.class);
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("+01:23"));
        AtopSplit split = new AtopSplit(HostAddress.fromParts("localhost", 123), now.toEpochSecond(), now.getZone().getId());
        AtopSplit decoded = codec.fromJson(codec.toJson(split));
        assertThat(decoded.getHost()).isEqualTo(split.getHost());
        assertThat(decoded.getDate()).isEqualTo(split.getDate());
        assertThat(decoded.getEpochSeconds()).isEqualTo(split.getEpochSeconds());
        assertThat(decoded.getTimeZoneId()).isEqualTo(split.getTimeZoneId());
    }
}
