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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static io.trino.cli.FormatUtils.formatCount;
import static io.trino.cli.FormatUtils.formatCountRate;
import static io.trino.cli.FormatUtils.formatDataRate;
import static io.trino.cli.FormatUtils.formatDataSize;
import static io.trino.cli.FormatUtils.formatFinalTime;
import static io.trino.cli.FormatUtils.formatProgressBar;
import static io.trino.cli.FormatUtils.pluralize;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestFormatUtils
{
    @Test
    public void testFormatCount()
    {
        assertEquals(formatCount(1L), "1");
        assertEquals(formatCount(12L), "12");
        assertEquals(formatCount(123L), "123");
        assertEquals(formatCount(1234L), "1.23K");
        assertEquals(formatCount(12345L), "12.3K");
        assertEquals(formatCount(123456L), "123K");
        assertEquals(formatCount(1234567L), "1.23M");
        assertEquals(formatCount(12345678L), "12.3M");
        assertEquals(formatCount(123456789L), "123M");
        assertEquals(formatCount(1234567890L), "1.23B");
        assertEquals(formatCount(12345678901L), "12.3B");
        assertEquals(formatCount(123456789012L), "123B");
        assertEquals(formatCount(1234567890123L), "1.23T");
        assertEquals(formatCount(12345678901234L), "12.3T");
        assertEquals(formatCount(123456789012345L), "123T");
        assertEquals(formatCount(1234567890123456L), "1.23Q");
        assertEquals(formatCount(12345678901234567L), "12.3Q");
        assertEquals(formatCount(123456789012345678L), "123Q");
        assertEquals(formatCount(1234567890123456789L), "1235Q");
    }

    @Test
    public void testFormatCountRate()
    {
        assertEquals(formatCountRate(0.0000000001D, Duration.valueOf("1ns"), false), "0");
        assertEquals(formatCountRate(0.0000000001D, Duration.valueOf("1ns"), true), "0/s");
        assertEquals(formatCountRate(0.000000001D, Duration.valueOf("1ns"), false), "1");
        assertEquals(formatCountRate(0.000000001D, Duration.valueOf("1ns"), true), "1/s");
        assertEquals(formatCountRate(0.0000000015D, Duration.valueOf("1ns"), false), "1");
        assertEquals(formatCountRate(0.0000000015D, Duration.valueOf("1ns"), true), "1/s");
        assertEquals(formatCountRate(1D, Duration.valueOf("1ns"), false), "1000M");
        assertEquals(formatCountRate(1D, Duration.valueOf("1ns"), true), "1000M/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1ns"), false), "10B");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1ns"), true), "10B/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10ns"), false), "1000M");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10ns"), true), "1000M/s");

        assertEquals(formatCountRate(0.0000001D, Duration.valueOf("1us"), false), "0");
        assertEquals(formatCountRate(0.0000001D, Duration.valueOf("1us"), true), "0/s");
        assertEquals(formatCountRate(0.000001D, Duration.valueOf("1us"), false), "1");
        assertEquals(formatCountRate(0.000001D, Duration.valueOf("1us"), true), "1/s");
        assertEquals(formatCountRate(0.0000015D, Duration.valueOf("1us"), false), "1");
        assertEquals(formatCountRate(0.0000015D, Duration.valueOf("1us"), true), "1/s");
        assertEquals(formatCountRate(1D, Duration.valueOf("1us"), false), "1000K");
        assertEquals(formatCountRate(1D, Duration.valueOf("1us"), true), "1000K/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1us"), false), "10M");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1us"), true), "10M/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10us"), false), "1000K");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10us"), true), "1000K/s");

        assertEquals(formatCountRate(0.0001D, Duration.valueOf("1ms"), false), "0");
        assertEquals(formatCountRate(0.0001D, Duration.valueOf("1ms"), true), "0/s");
        assertEquals(formatCountRate(0.001D, Duration.valueOf("1ms"), false), "1");
        assertEquals(formatCountRate(0.001D, Duration.valueOf("1ms"), true), "1/s");
        assertEquals(formatCountRate(0.0015D, Duration.valueOf("1ms"), false), "1");
        assertEquals(formatCountRate(0.0015D, Duration.valueOf("1ms"), true), "1/s");
        assertEquals(formatCountRate(1D, Duration.valueOf("1ms"), false), "1000");
        assertEquals(formatCountRate(1D, Duration.valueOf("1ms"), true), "1000/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1ms"), false), "10K");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1ms"), true), "10K/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10ms"), false), "1000");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10ms"), true), "1000/s");

        assertEquals(formatCountRate(0.1D, Duration.valueOf("1s"), false), "0");
        assertEquals(formatCountRate(0.1D, Duration.valueOf("1s"), true), "0/s");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("1s"), false), "1");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("1s"), true), "1/s");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("10s"), false), "0");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("10s"), true), "0/s");
        assertEquals(formatCountRate(1.5D, Duration.valueOf("1s"), false), "1");
        assertEquals(formatCountRate(1.5D, Duration.valueOf("1s"), true), "1/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1s"), false), "10");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("1s"), true), "10/s");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10s"), false), "1");
        assertEquals(formatCountRate(10.0D, Duration.valueOf("10s"), true), "1/s");

        assertEquals(formatCountRate(1.0D, Duration.valueOf("1m"), false), "0");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("1m"), true), "0/s");
        assertEquals(formatCountRate(60.0D, Duration.valueOf("1m"), false), "1");
        assertEquals(formatCountRate(60.0D, Duration.valueOf("1m"), true), "1/s");
        assertEquals(formatCountRate(600.0D, Duration.valueOf("1m"), false), "10");
        assertEquals(formatCountRate(600.0D, Duration.valueOf("1m"), true), "10/s");
        assertEquals(formatCountRate(100.0D, Duration.valueOf("10m"), false), "0");
        assertEquals(formatCountRate(100.0D, Duration.valueOf("10m"), true), "0/s");
        assertEquals(formatCountRate(600.0D, Duration.valueOf("10m"), false), "1");
        assertEquals(formatCountRate(600.0D, Duration.valueOf("10m"), true), "1/s");
        assertEquals(formatCountRate(6000.0D, Duration.valueOf("10m"), false), "10");
        assertEquals(formatCountRate(6000.0D, Duration.valueOf("10m"), true), "10/s");

        assertEquals(formatCountRate(1.0D, Duration.valueOf("1h"), false), "0");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("1h"), true), "0/s");
        assertEquals(formatCountRate(3600.0D, Duration.valueOf("1h"), false), "1");
        assertEquals(formatCountRate(3600.0D, Duration.valueOf("1h"), true), "1/s");
        assertEquals(formatCountRate(36000.0D, Duration.valueOf("1h"), false), "10");
        assertEquals(formatCountRate(36000.0D, Duration.valueOf("1h"), true), "10/s");
        assertEquals(formatCountRate(100.0D, Duration.valueOf("10h"), false), "0");
        assertEquals(formatCountRate(100.0D, Duration.valueOf("10h"), true), "0/s");
        assertEquals(formatCountRate(36000.0D, Duration.valueOf("10h"), false), "1");
        assertEquals(formatCountRate(36000.0D, Duration.valueOf("10h"), true), "1/s");
        assertEquals(formatCountRate(360000.0D, Duration.valueOf("10h"), false), "10");
        assertEquals(formatCountRate(360000.0D, Duration.valueOf("10h"), true), "10/s");

        assertEquals(formatCountRate(1.0D, Duration.valueOf("1d"), false), "0");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("1d"), true), "0/s");
        assertEquals(formatCountRate(86400.0D, Duration.valueOf("1d"), false), "1");
        assertEquals(formatCountRate(86400.0D, Duration.valueOf("1d"), true), "1/s");
        assertEquals(formatCountRate(864000.0D, Duration.valueOf("1d"), false), "10");
        assertEquals(formatCountRate(864000.0D, Duration.valueOf("1d"), true), "10/s");
        assertEquals(formatCountRate(86400.0D, Duration.valueOf("10d"), false), "0");
        assertEquals(formatCountRate(86400.0D, Duration.valueOf("10d"), true), "0/s");
        assertEquals(formatCountRate(864000.0D, Duration.valueOf("10d"), false), "1");
        assertEquals(formatCountRate(864000.0D, Duration.valueOf("10d"), true), "1/s");
        assertEquals(formatCountRate(8640000.0D, Duration.valueOf("10d"), false), "10");
        assertEquals(formatCountRate(8640000.0D, Duration.valueOf("10d"), true), "10/s");

        assertEquals(formatCountRate(Double.NaN, Duration.valueOf("1s"), false), "0");
        assertEquals(formatCountRate(Double.NaN, Duration.valueOf("1s"), true), "0/s");
        assertEquals(formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), false), "0");
        assertEquals(formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), true), "0/s");
        assertEquals(formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), false), "0");
        assertEquals(formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), true), "0/s");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("0s"), false), "0");
        assertEquals(formatCountRate(1.0D, Duration.valueOf("0s"), true), "0/s");
        assertEquals(formatCountRate(-1.0D, Duration.valueOf("0s"), false), "0");
        assertEquals(formatCountRate(-1.0D, Duration.valueOf("0s"), true), "0/s");
    }

    @Test
    public void testFormatDataSize()
    {
        assertEquals(formatDataSize(DataSize.ofBytes(1L), false), "1B");
        assertEquals(formatDataSize(DataSize.ofBytes(1L), true), "1B");
        assertEquals(formatDataSize(DataSize.ofBytes(12L), false), "12B");
        assertEquals(formatDataSize(DataSize.ofBytes(12L), true), "12B");
        assertEquals(formatDataSize(DataSize.ofBytes(123L), false), "123B");
        assertEquals(formatDataSize(DataSize.ofBytes(123L), true), "123B");
        assertEquals(formatDataSize(DataSize.ofBytes(1234L), false), "1.21K");
        assertEquals(formatDataSize(DataSize.ofBytes(1234L), true), "1.21KB");
        assertEquals(formatDataSize(DataSize.ofBytes(12345L), false), "12.1K");
        assertEquals(formatDataSize(DataSize.ofBytes(12345L), true), "12.1KB");
        assertEquals(formatDataSize(DataSize.ofBytes(123456L), false), "121K");
        assertEquals(formatDataSize(DataSize.ofBytes(123456L), true), "121KB");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567L), false), "1.18M");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567L), true), "1.18MB");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678L), false), "11.8M");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678L), true), "11.8MB");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789L), false), "118M");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789L), true), "118MB");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890L), false), "1.15G");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890L), true), "1.15GB");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901L), false), "11.5G");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901L), true), "11.5GB");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012L), false), "115G");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012L), true), "115GB");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123L), false), "1.12T");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123L), true), "1.12TB");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901234L), false), "11.2T");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901234L), true), "11.2TB");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012345L), false), "112T");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012345L), true), "112TB");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123456L), false), "1.1P");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123456L), true), "1.1PB");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901234567L), false), "11P");
        assertEquals(formatDataSize(DataSize.ofBytes(12345678901234567L), true), "11PB");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012345678L), false), "110P");
        assertEquals(formatDataSize(DataSize.ofBytes(123456789012345678L), true), "110PB");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123456789L), false), "1097P");
        assertEquals(formatDataSize(DataSize.ofBytes(1234567890123456789L), true), "1097PB");
    }

    @Test
    public void testFormatDataRate()
    {
        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), false), "954M");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), true), "954MB/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), false), "9.31G");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), true), "9.31GB/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), false), "954M");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), true), "954MB/s");

        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), false), "977K");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), true), "977KB/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), false), "9.54M");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), true), "9.54MB/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), false), "977K");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), true), "977KB/s");

        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), false), "1000B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), true), "1000B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), false), "9.77K");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), true), "9.77KB/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), false), "1000B");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), true), "1000B/s");

        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), true), "10B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), true), "1B/s");

        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), true), "10B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), true), "10B/s");

        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), true), "10B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), true), "10B/s");

        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), true), "10B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), true), "0B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), false), "1B");
        assertEquals(formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), true), "1B/s");
        assertEquals(formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), false), "10B");
        assertEquals(formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), true), "10B/s");

        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), false), "0B");
        assertEquals(formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), true), "0B/s");
    }

    @Test
    public void testPluralize()
    {
        assertEquals(pluralize("foo", 0), "foos");
        assertEquals(pluralize("foo", 1), "foo");
        assertEquals(pluralize("foo", 2), "foos");
    }

    @Test
    public void testFormatFinalTime()
    {
        assertEquals(formatFinalTime(Duration.valueOf("0us")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0ns")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0ms")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0s")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0m")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0h")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("0d")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("1us")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("1ns")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("1ms")), "0.00");
        assertEquals(formatFinalTime(Duration.valueOf("10ms")), "0.01");
        assertEquals(formatFinalTime(Duration.valueOf("100ms")), "0.10");
        assertEquals(formatFinalTime(Duration.valueOf("1s")), "1.00");
        assertEquals(formatFinalTime(Duration.valueOf("10s")), "10.00");
        assertEquals(formatFinalTime(Duration.valueOf("1m")), "1:00");
        assertEquals(formatFinalTime(Duration.valueOf("61s")), "1:01");
        assertEquals(formatFinalTime(Duration.valueOf("1h")), "60:00");
        assertEquals(formatFinalTime(Duration.valueOf("61m")), "61:00");
        assertEquals(formatFinalTime(Duration.valueOf("1d")), "1440:00");
        assertEquals(formatFinalTime(Duration.valueOf("25h")), "1500:00");
    }

    @Test
    public void testFormatIndeterminateProgressBar()
    {
        assertEquals(formatProgressBar(10, 0), "<=>       ");
        assertEquals(formatProgressBar(10, 1), " <=>      ");
        assertEquals(formatProgressBar(10, 7), "       <=>");
        assertEquals(formatProgressBar(10, 8), "      <=> ");
        assertEquals(formatProgressBar(10, 13), " <=>      ");
        assertEquals(formatProgressBar(10, 14), "<=>       ");
    }

    @Test
    public void testInvalidIndeterminateProgressBar()
    {
        assertThatThrownBy(() -> formatProgressBar(10, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid count: -1");
    }

    @Test
    public void testFormatProgressBar()
    {
        assertEquals(formatProgressBar(10, 0, 0, 0), "          ");
        assertEquals(formatProgressBar(10, 0, 0, 10), "          ");
        assertEquals(formatProgressBar(10, 1, 1, 10), "=>        ");
        assertEquals(formatProgressBar(10, 1, 2, 10), "=>>       ");
        assertEquals(formatProgressBar(10, 2, 1, 10), "==>       ");
        assertEquals(formatProgressBar(10, 2, 2, 10), "==>>      ");
        assertEquals(formatProgressBar(10, 5, 5, 10), "=====>>>>>");
        assertEquals(formatProgressBar(10, 10, 0, 10), "==========");
        assertEquals(formatProgressBar(10, 0, 10, 10), ">>>>>>>>>>");
        assertEquals(formatProgressBar(10, 6, 6, 10), "======>>>>");
        assertEquals(formatProgressBar(10, 12, 0, 10), "==========");
        assertEquals(formatProgressBar(10, 0, 12, 10), ">>>>>>>>>>");
    }

    @Test
    public void testInvalidProgressBar()
    {
        assertThatThrownBy(() -> formatProgressBar(10, -10, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid count: -9");
    }
}
