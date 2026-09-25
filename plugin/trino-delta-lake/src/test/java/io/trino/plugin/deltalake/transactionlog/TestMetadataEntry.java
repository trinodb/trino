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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestMetadataEntry
{
    @Test
    void testLogCleanupDefaults()
    {
        MetadataEntry metadata = MetadataEntry.builder().build();
        assertThat(metadata.isExpiredLogCleanupEnabled()).isTrue();
        assertThat(metadata.getLogRetentionDuration()).isEqualTo(Duration.ofDays(30));
        metadata = MetadataEntry.builder().setConfiguration(ImmutableMap.of()).build();
        assertThat(metadata.isExpiredLogCleanupEnabled()).isTrue();
        assertThat(metadata.getLogRetentionDuration()).isEqualTo(Duration.ofDays(30));
    }

    @Test
    void testLogCleanupEnabled()
    {
        for (String value : List.of("true", "TRUE", "false", "FALSE")) {
            assertThat(MetadataEntry.builder().setConfiguration(ImmutableMap.of("delta.enableExpiredLogCleanup", value)).build().isExpiredLogCleanupEnabled())
                    .isEqualTo(Boolean.parseBoolean(value));
        }
    }

    @Test
    void testInvalidLogCleanupEnabled()
    {
        assertThatThrownBy(() -> MetadataEntry.builder().setConfiguration(ImmutableMap.of("delta.enableExpiredLogCleanup", "yes")).build().isExpiredLogCleanupEnabled())
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("delta.enableExpiredLogCleanup");
    }

    @Test
    void testLogRetentionDuration()
    {
        Map<String, Long> intervals = ImmutableMap.<String, Long>builder()
                .put("interval 30 days", 2592000000L)
                .put("2 weeks", 1209600000L)
                .put("INTERVAL 1 WEEK 2 DAYS 3 HOURS 4 MINUTES 5 SECONDS 6 MILLISECONDS", 788645006L)
                .put("interval 1 day -1 hour", 82800000L)
                .put("0 seconds", 0L)
                .put("+ 1 second", 1000L)
                .put("1.5 seconds", 1500L)
                .put(".001 second", 1L)
                .put("1000 microseconds", 1L)
                .buildOrThrow();
        intervals.forEach((value, milliseconds) -> assertThat(metadataWithRetention(value).getLogRetentionDuration())
                .as(value)
                .isEqualTo(Duration.ofMillis(milliseconds)));
    }

    @Test
    void testInvalidLogRetentionDuration()
    {
        for (String value : List.of("", "interval", "30d", "garbage", "1 month", "1 year", "-1 day", "-0.000001 seconds", "1.5 days", "1 day trailing", "1 day1 hour", "1 nanosecond", "1.1234567890 seconds", "9223372036854775807 days", "9223372036854775807 seconds 1 second")) {
            assertThatThrownBy(() -> metadataWithRetention(value).getLogRetentionDuration())
                    .as(value)
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("delta.logRetentionDuration");
        }
    }

    private static MetadataEntry metadataWithRetention(String value)
    {
        return MetadataEntry.builder().setConfiguration(ImmutableMap.of("delta.logRetentionDuration", value)).build();
    }
}
