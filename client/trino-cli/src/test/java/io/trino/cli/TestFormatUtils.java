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
package io.trino.cli;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.trino.cli.FormatUtils.formatCount;
import static io.trino.cli.FormatUtils.formatCountRate;
import static io.trino.cli.FormatUtils.formatDataRate;
import static io.trino.cli.FormatUtils.formatDataSize;
import static io.trino.cli.FormatUtils.formatFinalTime;
import static io.trino.cli.FormatUtils.formatProgressBar;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFormatUtils
{
    @Test
    public void testFormatCount()
    {
        assertThat(formatCount(1L)).isEqualTo("1");
        assertThat(formatCount(12L)).isEqualTo("12");
        assertThat(formatCount(123L)).isEqualTo("123");
        assertThat(formatCount(1234L)).isEqualTo("1.23K");
        assertThat(formatCount(12345L)).isEqualTo("12.3K");
        assertThat(formatCount(123456L)).isEqualTo("123K");
        assertThat(formatCount(1234567L)).isEqualTo("1.23M");
        assertThat(formatCount(12345678L)).isEqualTo("12.3M");
        assertThat(formatCount(123456789L)).isEqualTo("123M");
        assertThat(formatCount(1234567890L)).isEqualTo("1.23B");
        assertThat(formatCount(12345678901L)).isEqualTo("12.3B");
        assertThat(formatCount(123456789012L)).isEqualTo("123B");
        assertThat(formatCount(1234567890123L)).isEqualTo("1.23T");
        assertThat(formatCount(12345678901234L)).isEqualTo("12.3T");
        assertThat(formatCount(123456789012345L)).isEqualTo("123T");
        assertThat(formatCount(1234567890123456L)).isEqualTo("1.23Q");
        assertThat(formatCount(12345678901234567L)).isEqualTo("12.3Q");
        assertThat(formatCount(123456789012345678L)).isEqualTo("123Q");
        assertThat(formatCount(1234567890123456789L)).isEqualTo("1235Q");
    }

    @Test
    public void testFormatCountRate()
    {
        assertThat(formatCountRate(0.0000000001D, Duration.ofNanos(1), false))
                .isEqualTo("0");
        assertThat(formatCountRate(0.0000000001D, Duration.ofNanos(1), true))
                .isEqualTo("0/s");
        assertThat(formatCountRate(0.000000001D, Duration.ofNanos(1), false))
                .isEqualTo("1");
        assertThat(formatCountRate(0.000000001D, Duration.ofNanos(1), true))
                .isEqualTo("1/s");
        assertThat(formatCountRate(0.0000000015D, Duration.ofNanos(1), false))
                .isEqualTo("1");
        assertThat(formatCountRate(0.0000000015D, Duration.ofNanos(1), true))
                .isEqualTo("1/s");
        assertThat(formatCountRate(1D, Duration.ofNanos(1), false)).isEqualTo("1000M");
        assertThat(formatCountRate(1D, Duration.ofNanos(1), true)).isEqualTo("1000M/s");
        assertThat(formatCountRate(10.0D, Duration.ofNanos(1), false)).isEqualTo("10B");
        assertThat(formatCountRate(10.0D, Duration.ofNanos(1), true)).isEqualTo("10B/s");
        assertThat(formatCountRate(10.0D, Duration.ofNanos(10), false)).isEqualTo("1000M");
        assertThat(formatCountRate(10.0D, Duration.ofNanos(10), true)).isEqualTo("1000M/s");

        assertThat(formatCountRate(0.0000001D, Duration.ZERO, false)).isEqualTo("0");
        assertThat(formatCountRate(0.0000001D, Duration.ZERO, true)).isEqualTo("0/s");
        assertThat(formatCountRate(0.000000001D, Duration.ofNanos(1), false)).isEqualTo("1");
        assertThat(formatCountRate(0.000000001D, Duration.ofNanos(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(0.001D, Duration.ofNanos(1), false)).isEqualTo("1000K");
        assertThat(formatCountRate(0.001D, Duration.ofNanos(1), true)).isEqualTo("1000K/s");
        assertThat(formatCountRate(0.01D, Duration.ofNanos(1), false)).isEqualTo("10M");
        assertThat(formatCountRate(0.01D, Duration.ofNanos(1), true)).isEqualTo("10M/s");
        assertThat(formatCountRate(0.1D, Duration.ofNanos(1), false)).isEqualTo("100M");
        assertThat(formatCountRate(0.1D, Duration.ofNanos(1), true)).isEqualTo("100M/s");

        assertThat(formatCountRate(0.0001D, Duration.ofMillis(1), false)).isEqualTo("0");
        assertThat(formatCountRate(0.0001D, Duration.ofMillis(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(0.001D, Duration.ofMillis(1), false)).isEqualTo("1");
        assertThat(formatCountRate(0.001D, Duration.ofMillis(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(0.0015D, Duration.ofMillis(1), false)).isEqualTo("1");
        assertThat(formatCountRate(0.0015D, Duration.ofMillis(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(1D, Duration.ofMillis(1), false)).isEqualTo("1000");
        assertThat(formatCountRate(1D, Duration.ofMillis(1), true)).isEqualTo("1000/s");
        assertThat(formatCountRate(10.0D, Duration.ofMillis(1), false)).isEqualTo("10K");
        assertThat(formatCountRate(10.0D, Duration.ofMillis(1), true)).isEqualTo("10K/s");
        assertThat(formatCountRate(10.0D, Duration.ofMillis(10), false)).isEqualTo("1000");
        assertThat(formatCountRate(10.0D, Duration.ofMillis(10), true)).isEqualTo("1000/s");

        assertThat(formatCountRate(0.1D, Duration.ofSeconds(1), false)).isEqualTo("0");
        assertThat(formatCountRate(0.1D, Duration.ofSeconds(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(1.0D, Duration.ofSeconds(1), false)).isEqualTo("1");
        assertThat(formatCountRate(1.0D, Duration.ofSeconds(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(1.0D, Duration.ofSeconds(10), false)).isEqualTo("0");
        assertThat(formatCountRate(1.0D, Duration.ofSeconds(10), true)).isEqualTo("0/s");
        assertThat(formatCountRate(1.5D, Duration.ofSeconds(1), false)).isEqualTo("1");
        assertThat(formatCountRate(1.5D, Duration.ofSeconds(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(10.0D, Duration.ofSeconds(1), false)).isEqualTo("10");
        assertThat(formatCountRate(10.0D, Duration.ofSeconds(1), true)).isEqualTo("10/s");
        assertThat(formatCountRate(10.0D, Duration.ofSeconds(10), false)).isEqualTo("1");
        assertThat(formatCountRate(10.0D, Duration.ofSeconds(10), true)).isEqualTo("1/s");

        assertThat(formatCountRate(1.0D, Duration.ofMinutes(1), false)).isEqualTo("0");
        assertThat(formatCountRate(1.0D, Duration.ofMinutes(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(60.0D, Duration.ofMinutes(1), false)).isEqualTo("1");
        assertThat(formatCountRate(60.0D, Duration.ofMinutes(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(600.0D, Duration.ofMinutes(1), false)).isEqualTo("10");
        assertThat(formatCountRate(600.0D, Duration.ofMinutes(1), true)).isEqualTo("10/s");
        assertThat(formatCountRate(100.0D, Duration.ofMinutes(10), false)).isEqualTo("0");
        assertThat(formatCountRate(100.0D, Duration.ofMinutes(10), true)).isEqualTo("0/s");
        assertThat(formatCountRate(600.0D, Duration.ofMinutes(10), false)).isEqualTo("1");
        assertThat(formatCountRate(600.0D, Duration.ofMinutes(10), true)).isEqualTo("1/s");
        assertThat(formatCountRate(6000.0D, Duration.ofMinutes(10), false)).isEqualTo("10");
        assertThat(formatCountRate(6000.0D, Duration.ofMinutes(10), true)).isEqualTo("10/s");

        assertThat(formatCountRate(1.0D, Duration.ofHours(1), false)).isEqualTo("0");
        assertThat(formatCountRate(1.0D, Duration.ofHours(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(3600.0D, Duration.ofHours(1), false)).isEqualTo("1");
        assertThat(formatCountRate(3600.0D, Duration.ofHours(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(36000.0D, Duration.ofHours(1), false)).isEqualTo("10");
        assertThat(formatCountRate(36000.0D, Duration.ofHours(1), true)).isEqualTo("10/s");
        assertThat(formatCountRate(100.0D, Duration.ofHours(10), false)).isEqualTo("0");
        assertThat(formatCountRate(100.0D, Duration.ofHours(10), true)).isEqualTo("0/s");
        assertThat(formatCountRate(36000.0D, Duration.ofHours(10), false)).isEqualTo("1");
        assertThat(formatCountRate(36000.0D, Duration.ofHours(10), true)).isEqualTo("1/s");
        assertThat(formatCountRate(360000.0D, Duration.ofHours(10), false)).isEqualTo("10");
        assertThat(formatCountRate(360000.0D, Duration.ofHours(10), true)).isEqualTo("10/s");

        assertThat(formatCountRate(1.0D, Duration.ofDays(1), false)).isEqualTo("0");
        assertThat(formatCountRate(1.0D, Duration.ofDays(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(86400.0D, Duration.ofDays(1), false)).isEqualTo("1");
        assertThat(formatCountRate(86400.0D, Duration.ofDays(1), true)).isEqualTo("1/s");
        assertThat(formatCountRate(864000.0D, Duration.ofDays(1), false)).isEqualTo("10");
        assertThat(formatCountRate(864000.0D, Duration.ofDays(1), true)).isEqualTo("10/s");
        assertThat(formatCountRate(86400.0D, Duration.ofDays(10), false)).isEqualTo("0");
        assertThat(formatCountRate(86400.0D, Duration.ofDays(10), true)).isEqualTo("0/s");
        assertThat(formatCountRate(864000.0D, Duration.ofDays(10), false)).isEqualTo("1");
        assertThat(formatCountRate(864000.0D, Duration.ofDays(10), true)).isEqualTo("1/s");
        assertThat(formatCountRate(8640000.0D, Duration.ofDays(10), false)).isEqualTo("10");
        assertThat(formatCountRate(8640000.0D, Duration.ofDays(10), true)).isEqualTo("10/s");

        assertThat(formatCountRate(NaN, Duration.ofSeconds(1), false)).isEqualTo("0");
        assertThat(formatCountRate(NaN, Duration.ofSeconds(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(POSITIVE_INFINITY, Duration.ofSeconds(1), false)).isEqualTo("0");
        assertThat(formatCountRate(POSITIVE_INFINITY, Duration.ofSeconds(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(NEGATIVE_INFINITY, Duration.ofSeconds(1), false)).isEqualTo("0");
        assertThat(formatCountRate(NEGATIVE_INFINITY, Duration.ofSeconds(1), true)).isEqualTo("0/s");
        assertThat(formatCountRate(1.0D, Duration.ZERO, false)).isEqualTo("0");
        assertThat(formatCountRate(1.0D, Duration.ZERO, true)).isEqualTo("0/s");
        assertThat(formatCountRate(-1.0D, Duration.ZERO, false)).isEqualTo("0");
        assertThat(formatCountRate(-1.0D, Duration.ZERO, true)).isEqualTo("0/s");
    }

    @Test
    public void testFormatDataSizeBinary()
    {
        assertThat(formatDataSize(1L, false, false)).isEqualTo("1B");
        assertThat(formatDataSize(1L, true, false)).isEqualTo("1B");
        assertThat(formatDataSize(12L, false, false)).isEqualTo("12B");
        assertThat(formatDataSize(12L, true, false)).isEqualTo("12B");
        assertThat(formatDataSize(123L, false, false)).isEqualTo("123B");
        assertThat(formatDataSize(123L, true, false)).isEqualTo("123B");
        assertThat(formatDataSize(1234L, false, false)).isEqualTo("1.21Ki");
        assertThat(formatDataSize(1234L, true, false)).isEqualTo("1.21KiB");
        assertThat(formatDataSize(12345L, false, false)).isEqualTo("12.1Ki");
        assertThat(formatDataSize(12345L, true, false)).isEqualTo("12.1KiB");
        assertThat(formatDataSize(123456L, false, false)).isEqualTo("121Ki");
        assertThat(formatDataSize(123456L, true, false)).isEqualTo("121KiB");
        assertThat(formatDataSize(1234567L, false, false)).isEqualTo("1.18Mi");
        assertThat(formatDataSize(1234567L, true, false)).isEqualTo("1.18MiB");
        assertThat(formatDataSize(12345678L, false, false)).isEqualTo("11.8Mi");
        assertThat(formatDataSize(12345678L, true, false)).isEqualTo("11.8MiB");
        assertThat(formatDataSize(123456789L, false, false)).isEqualTo("118Mi");
        assertThat(formatDataSize(123456789L, true, false)).isEqualTo("118MiB");
        assertThat(formatDataSize(1234567890L, false, false)).isEqualTo("1.15Gi");
        assertThat(formatDataSize(1234567890L, true, false)).isEqualTo("1.15GiB");
        assertThat(formatDataSize(12345678901L, false, false)).isEqualTo("11.5Gi");
        assertThat(formatDataSize(12345678901L, true, false)).isEqualTo("11.5GiB");
        assertThat(formatDataSize(123456789012L, false, false)).isEqualTo("115Gi");
        assertThat(formatDataSize(123456789012L, true, false)).isEqualTo("115GiB");
        assertThat(formatDataSize(1234567890123L, false, false)).isEqualTo("1.12Ti");
        assertThat(formatDataSize(1234567890123L, true, false)).isEqualTo("1.12TiB");
        assertThat(formatDataSize(12345678901234L, false, false)).isEqualTo("11.2Ti");
        assertThat(formatDataSize(12345678901234L, true, false)).isEqualTo("11.2TiB");
        assertThat(formatDataSize(123456789012345L, false, false)).isEqualTo("112Ti");
        assertThat(formatDataSize(123456789012345L, true, false)).isEqualTo("112TiB");
        assertThat(formatDataSize(1234567890123456L, false, false)).isEqualTo("1.1Pi");
        assertThat(formatDataSize(1234567890123456L, true, false)).isEqualTo("1.1PiB");
        assertThat(formatDataSize(12345678901234567L, false, false)).isEqualTo("11Pi");
        assertThat(formatDataSize(12345678901234567L, true, false)).isEqualTo("11PiB");
        assertThat(formatDataSize(123456789012345678L, false, false)).isEqualTo("110Pi");
        assertThat(formatDataSize(123456789012345678L, true, false)).isEqualTo("110PiB");
        assertThat(formatDataSize(1234567890123456789L, false, false)).isEqualTo("1097Pi");
        assertThat(formatDataSize(1234567890123456789L, true, false)).isEqualTo("1097PiB");
    }

    @Test
    public void testFormatDataSizeDecimal()
    {
        assertThat(formatDataSize(1L, false, true)).isEqualTo("1B");
        assertThat(formatDataSize(1L, true, true)).isEqualTo("1B");
        assertThat(formatDataSize(12L, false, true)).isEqualTo("12B");
        assertThat(formatDataSize(12L, true, true)).isEqualTo("12B");
        assertThat(formatDataSize(123L, false, true)).isEqualTo("123B");
        assertThat(formatDataSize(123L, true, true)).isEqualTo("123B");
        assertThat(formatDataSize(1234L, false, true)).isEqualTo("1.23K");
        assertThat(formatDataSize(1234L, true, true)).isEqualTo("1.23KB");
        assertThat(formatDataSize(12345L, false, true)).isEqualTo("12.3K");
        assertThat(formatDataSize(12345L, true, true)).isEqualTo("12.3KB");
        assertThat(formatDataSize(123456L, false, true)).isEqualTo("123K");
        assertThat(formatDataSize(123456L, true, true)).isEqualTo("123KB");
        assertThat(formatDataSize(1234567L, false, true)).isEqualTo("1.23M");
        assertThat(formatDataSize(1234567L, true, true)).isEqualTo("1.23MB");
        assertThat(formatDataSize(12345678L, false, true)).isEqualTo("12.3M");
        assertThat(formatDataSize(12345678L, true, true)).isEqualTo("12.3MB");
        assertThat(formatDataSize(123456789L, false, true)).isEqualTo("123M");
        assertThat(formatDataSize(123456789L, true, true)).isEqualTo("123MB");
        assertThat(formatDataSize(1234567890L, false, true)).isEqualTo("1.23G");
        assertThat(formatDataSize(1234567890L, true, true)).isEqualTo("1.23GB");
        assertThat(formatDataSize(12345678901L, false, true)).isEqualTo("12.3G");
        assertThat(formatDataSize(12345678901L, true, true)).isEqualTo("12.3GB");
        assertThat(formatDataSize(123456789012L, false, true)).isEqualTo("123G");
        assertThat(formatDataSize(123456789012L, true, true)).isEqualTo("123GB");
        assertThat(formatDataSize(1234567890123L, false, true)).isEqualTo("1.23T");
        assertThat(formatDataSize(1234567890123L, true, true)).isEqualTo("1.23TB");
        assertThat(formatDataSize(12345678901234L, false, true)).isEqualTo("12.3T");
        assertThat(formatDataSize(12345678901234L, true, true)).isEqualTo("12.3TB");
        assertThat(formatDataSize(123456789012345L, false, true)).isEqualTo("123T");
        assertThat(formatDataSize(123456789012345L, true, true)).isEqualTo("123TB");
        assertThat(formatDataSize(1234567890123456L, false, true)).isEqualTo("1.23P");
        assertThat(formatDataSize(1234567890123456L, true, true)).isEqualTo("1.23PB");
        assertThat(formatDataSize(12345678901234567L, false, true)).isEqualTo("12.3P");
        assertThat(formatDataSize(12345678901234567L, true, true)).isEqualTo("12.3PB");
        assertThat(formatDataSize(123456789012345678L, false, true)).isEqualTo("123P");
        assertThat(formatDataSize(123456789012345678L, true, true)).isEqualTo("123PB");
        assertThat(formatDataSize(1234567890123456789L, false, true)).isEqualTo("1235P");
        assertThat(formatDataSize(1234567890123456789L, true, true)).isEqualTo("1235PB");
    }

    @Test
    public void testFormatDataRateBinary()
    {
        assertThat(formatDataRate(0, Duration.ofNanos(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofNanos(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofNanos(1), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(1, Duration.ofNanos(1), true, true)).isEqualTo("1GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(1), false, true)).isEqualTo("10G");
        assertThat(formatDataRate(10, Duration.ofNanos(1), true, true)).isEqualTo("10GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(10), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(10, Duration.ofNanos(10), true, true)).isEqualTo("1GB/s");

        assertThat(formatDataRate(0, Duration.ZERO, false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ZERO, true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofNanos(1), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(1, Duration.ofNanos(1), true, true)).isEqualTo("1GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(1), false, true)).isEqualTo("10G");
        assertThat(formatDataRate(10, Duration.ofNanos(1), true, true)).isEqualTo("10GB/s");
        assertThat(formatDataRate(100, Duration.ofNanos(1), false, true)).isEqualTo("100G");
        assertThat(formatDataRate(100, Duration.ofNanos(1), true, true)).isEqualTo("100GB/s");

        assertThat(formatDataRate(0, Duration.ofMillis(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofMillis(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofMillis(1), false, true)).isEqualTo("1K");
        assertThat(formatDataRate(1, Duration.ofMillis(1), true, true)).isEqualTo("1KB/s");
        assertThat(formatDataRate(10, Duration.ofMillis(1), false, true)).isEqualTo("10K");
        assertThat(formatDataRate(10, Duration.ofMillis(1), true, true)).isEqualTo("10KB/s");
        assertThat(formatDataRate(10, Duration.ofMillis(10), false, true)).isEqualTo("1K");
        assertThat(formatDataRate(10, Duration.ofMillis(10), true, true)).isEqualTo("1KB/s");

        assertThat(formatDataRate(0, Duration.ofSeconds(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofSeconds(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofSeconds(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(1, Duration.ofSeconds(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(1, Duration.ofSeconds(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofSeconds(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(10, Duration.ofSeconds(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(10, Duration.ofSeconds(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(10, Duration.ofSeconds(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(10, Duration.ofSeconds(10), true, true)).isEqualTo("1B/s");

        assertThat(formatDataRate(1, Duration.ofMinutes(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofMinutes(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(60, Duration.ofMinutes(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(60, Duration.ofMinutes(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(600, Duration.ofMinutes(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(600, Duration.ofMinutes(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(100, Duration.ofMinutes(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(100, Duration.ofMinutes(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(600, Duration.ofMinutes(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(600, Duration.ofMinutes(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(6000, Duration.ofMinutes(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(6000, Duration.ofMinutes(10), true, true)).isEqualTo("10B/s");

        assertThat(formatDataRate(1, Duration.ofHours(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofHours(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(3600, Duration.ofHours(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(3600, Duration.ofHours(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(36000, Duration.ofHours(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(36000, Duration.ofHours(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(100, Duration.ofHours(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(100, Duration.ofHours(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(36000, Duration.ofHours(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(36000, Duration.ofHours(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(360000, Duration.ofHours(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(360000, Duration.ofHours(10), true, true)).isEqualTo("10B/s");

        assertThat(formatDataRate(1, Duration.ofDays(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofDays(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(86400, Duration.ofDays(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(86400, Duration.ofDays(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(864000, Duration.ofDays(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(864000, Duration.ofDays(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(86400, Duration.ofDays(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(86400, Duration.ofDays(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(864000, Duration.ofDays(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(864000, Duration.ofDays(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(8640000, Duration.ofDays(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(8640000, Duration.ofDays(10), true, true)).isEqualTo("10B/s");

        // Currently, these tests fail due to https://github.com/trinodb/trino/issues/13093
        // assertThat(FormatUtils.formatDataRate(DataSize.1, Duration.ZERO, false)).isEqualTo("0B");
        // assertThat(FormatUtils.formatDataRate(DataSize.1, Duration.ZERO, true)).isEqualTo("0B/s");
    }

    @Test
    public void testFormatDataRateDecimal()
    {
        assertThat(formatDataRate(0, Duration.ofNanos(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofNanos(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofNanos(1), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(1, Duration.ofNanos(1), true, true)).isEqualTo("1GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(1), false, true)).isEqualTo("10G");
        assertThat(formatDataRate(10, Duration.ofNanos(1), true, true)).isEqualTo("10GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(10), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(10, Duration.ofNanos(10), true, true)).isEqualTo("1GB/s");

        assertThat(formatDataRate(0, Duration.ZERO, false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ZERO, true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofNanos(1), false, true)).isEqualTo("1G");
        assertThat(formatDataRate(1, Duration.ofNanos(1), true, true)).isEqualTo("1GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(1), false, true)).isEqualTo("10G");
        assertThat(formatDataRate(10, Duration.ofNanos(1), true, true)).isEqualTo("10GB/s");
        assertThat(formatDataRate(10, Duration.ofNanos(1), false, true)).isEqualTo("10G");
        assertThat(formatDataRate(10, Duration.ofNanos(1), true, true)).isEqualTo("10GB/s");

        assertThat(formatDataRate(0, Duration.ofMillis(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofMillis(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofMillis(1), false, true)).isEqualTo("1K");
        assertThat(formatDataRate(1, Duration.ofMillis(1), true, true)).isEqualTo("1KB/s");
        assertThat(formatDataRate(10, Duration.ofMillis(1), false, true)).isEqualTo("10K");
        assertThat(formatDataRate(10, Duration.ofMillis(1), true, true)).isEqualTo("10KB/s");
        assertThat(formatDataRate(10, Duration.ofMillis(10), false, true)).isEqualTo("1K");
        assertThat(formatDataRate(10, Duration.ofMillis(10), true, true)).isEqualTo("1KB/s");

        assertThat(formatDataRate(0, Duration.ofSeconds(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(0, Duration.ofSeconds(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(1, Duration.ofSeconds(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(1, Duration.ofSeconds(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(1, Duration.ofSeconds(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofSeconds(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(10, Duration.ofSeconds(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(10, Duration.ofSeconds(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(10, Duration.ofSeconds(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(10, Duration.ofSeconds(10), true, true)).isEqualTo("1B/s");

        assertThat(formatDataRate(1, Duration.ofMinutes(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofMinutes(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(60, Duration.ofMinutes(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(60, Duration.ofMinutes(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(600, Duration.ofMinutes(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(600, Duration.ofMinutes(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(100, Duration.ofMinutes(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(100, Duration.ofMinutes(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(600, Duration.ofMinutes(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(600, Duration.ofMinutes(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(6000, Duration.ofMinutes(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(6000, Duration.ofMinutes(10), true, true)).isEqualTo("10B/s");

        assertThat(formatDataRate(1, Duration.ofHours(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofHours(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(3600, Duration.ofHours(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(3600, Duration.ofHours(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(36000, Duration.ofHours(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(36000, Duration.ofHours(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(100, Duration.ofHours(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(100, Duration.ofHours(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(36000, Duration.ofHours(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(36000, Duration.ofHours(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(360000, Duration.ofHours(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(360000, Duration.ofHours(10), true, true)).isEqualTo("10B/s");

        assertThat(formatDataRate(1, Duration.ofDays(1), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(1, Duration.ofDays(1), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(86400, Duration.ofDays(1), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(86400, Duration.ofDays(1), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(864000, Duration.ofDays(1), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(864000, Duration.ofDays(1), true, true)).isEqualTo("10B/s");
        assertThat(formatDataRate(86400, Duration.ofDays(10), false, true)).isEqualTo("0B");
        assertThat(formatDataRate(86400, Duration.ofDays(10), true, true)).isEqualTo("0B/s");
        assertThat(formatDataRate(864000, Duration.ofDays(10), false, true)).isEqualTo("1B");
        assertThat(formatDataRate(864000, Duration.ofDays(10), true, true)).isEqualTo("1B/s");
        assertThat(formatDataRate(8640000, Duration.ofDays(10), false, true)).isEqualTo("10B");
        assertThat(formatDataRate(8640000, Duration.ofDays(10), true, true)).isEqualTo("10B/s");

        assertThat(FormatUtils.formatDataRate(1, Duration.ZERO, false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(1, Duration.ZERO, true, true)).isEqualTo("0B/s");
    }

    @Test
    public void testPluralize()
    {
        assertThat(FormatUtils.pluralize("foo", 0)).isEqualTo("foos");
        assertThat(FormatUtils.pluralize("foo", 1)).isEqualTo("foo");
        assertThat(FormatUtils.pluralize("foo", 2)).isEqualTo("foos");
    }

    @Test
    public void testFormatFinalTime()
    {
        assertThat(formatFinalTime(Duration.ZERO)).isEqualTo("0.00");
        assertThat(formatFinalTime(Duration.ZERO)).isEqualTo("0.00");
        assertThat(formatFinalTime(Duration.ofNanos(1))).isEqualTo("0.00");
        assertThat(formatFinalTime(Duration.ofMillis(1))).isEqualTo("0.00");
        assertThat(formatFinalTime(Duration.ofMillis(10))).isEqualTo("0.01");
        assertThat(formatFinalTime(Duration.ofMillis(100))).isEqualTo("0.10");
        assertThat(formatFinalTime(Duration.ofSeconds(1))).isEqualTo("1.00");
        assertThat(formatFinalTime(Duration.ofSeconds(10))).isEqualTo("10.00");
        assertThat(formatFinalTime(Duration.ofMinutes(1))).isEqualTo("1:00");
        assertThat(formatFinalTime(Duration.ofSeconds(61))).isEqualTo("1:01");
        assertThat(formatFinalTime(Duration.ofHours(1))).isEqualTo("60:00");
        assertThat(formatFinalTime(Duration.ofMinutes(61))).isEqualTo("61:00");
        assertThat(formatFinalTime(Duration.ofDays(1))).isEqualTo("1440:00");
        assertThat(formatFinalTime(Duration.ofHours(25))).isEqualTo("1500:00");
    }

    @Test
    public void testFormatIndeterminateProgressBar()
    {
        assertThat(formatProgressBar(10, 0)).isEqualTo("<=>       ");
        assertThat(formatProgressBar(10, 1)).isEqualTo(" <=>      ");
        assertThat(formatProgressBar(10, 7)).isEqualTo("       <=>");
        assertThat(formatProgressBar(10, 8)).isEqualTo("      <=> ");
        assertThat(formatProgressBar(10, 13)).isEqualTo(" <=>      ");
        assertThat(formatProgressBar(10, 14)).isEqualTo("<=>       ");
    }

    @Test
    public void testInvalidIndeterminateProgressBar()
    {
        assertThatThrownBy(() -> formatProgressBar(10, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count is negative: -1");
    }

    @Test
    public void testFormatProgressBar()
    {
        assertThat(formatProgressBar(10, 0, 0)).isEqualTo("          ");
        assertThat(formatProgressBar(10, 10, 10)).isEqualTo("=>        ");
        assertThat(formatProgressBar(10, 10, 20)).isEqualTo("=>>       ");
        assertThat(formatProgressBar(10, 20, 10)).isEqualTo("==>       ");
        assertThat(formatProgressBar(10, 20, 20)).isEqualTo("==>>      ");
        assertThat(formatProgressBar(10, 50, 50)).isEqualTo("=====>>>>>");
        assertThat(formatProgressBar(10, 100, 0)).isEqualTo("==========");
        assertThat(formatProgressBar(10, 0, 100)).isEqualTo(">>>>>>>>>>");
        assertThat(formatProgressBar(10, 60, 60)).isEqualTo("======>>>>");
        assertThat(formatProgressBar(10, 120, 0)).isEqualTo("==========");
        assertThat(formatProgressBar(10, 0, 120)).isEqualTo(">>>>>>>>>>");
    }

    @Test
    public void testInvalidProgressBar()
    {
        assertThatThrownBy(() -> formatProgressBar(10, -100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("count is negative: -9");
    }
}
