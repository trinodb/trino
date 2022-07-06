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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class TestFormatUtils
{
    @Test
    public void testFormatCount()
    {
        assertEquals(FormatUtils.formatCount(1L), "1");
        assertEquals(FormatUtils.formatCount(12L), "12");
        assertEquals(FormatUtils.formatCount(123L), "123");
        assertEquals(FormatUtils.formatCount(1234L), "1.23K");
        assertEquals(FormatUtils.formatCount(12345L), "12.3K");
        assertEquals(FormatUtils.formatCount(123456L), "123K");
        assertEquals(FormatUtils.formatCount(1234567L), "1.23M");
        assertEquals(FormatUtils.formatCount(12345678L), "12.3M");
        assertEquals(FormatUtils.formatCount(123456789L), "123M");
        assertEquals(FormatUtils.formatCount(1234567890L), "1.23B");
        assertEquals(FormatUtils.formatCount(12345678901L), "12.3B");
        assertEquals(FormatUtils.formatCount(123456789012L), "123B");
        assertEquals(FormatUtils.formatCount(1234567890123L), "1.23T");
        assertEquals(FormatUtils.formatCount(12345678901234L), "12.3T");
        assertEquals(FormatUtils.formatCount(123456789012345L), "123T");
        assertEquals(FormatUtils.formatCount(1234567890123456L), "1.23Q");
        assertEquals(FormatUtils.formatCount(12345678901234567L), "12.3Q");
        assertEquals(FormatUtils.formatCount(123456789012345678L), "123Q");
        assertEquals(FormatUtils.formatCount(1234567890123456789L), "1235Q");
    }

    @Test
    public void formatCountRate()
    {
        assertEquals(FormatUtils.formatCountRate(0.0000000001D, Duration.valueOf("1ns"), false), "0");
        assertEquals(FormatUtils.formatCountRate(0.0000000001D, Duration.valueOf("1ns"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(0.000000001D, Duration.valueOf("1ns"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.000000001D, Duration.valueOf("1ns"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(0.0000000015D, Duration.valueOf("1ns"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.0000000015D, Duration.valueOf("1ns"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1ns"), false), "1000M");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1ns"), true), "1000M/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ns"), false), "10B");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ns"), true), "10B/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ns"), false), "1000M");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ns"), true), "1000M/s");

        assertEquals(FormatUtils.formatCountRate(0.0000001D, Duration.valueOf("1us"), false), "0");
        assertEquals(FormatUtils.formatCountRate(0.0000001D, Duration.valueOf("1us"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(0.000001D, Duration.valueOf("1us"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.000001D, Duration.valueOf("1us"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(0.0000015D, Duration.valueOf("1us"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.0000015D, Duration.valueOf("1us"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1us"), false), "1000K");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1us"), true), "1000K/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1us"), false), "10M");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1us"), true), "10M/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10us"), false), "1000K");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10us"), true), "1000K/s");

        assertEquals(FormatUtils.formatCountRate(0.0001D, Duration.valueOf("1ms"), false), "0");
        assertEquals(FormatUtils.formatCountRate(0.0001D, Duration.valueOf("1ms"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(0.001D, Duration.valueOf("1ms"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.001D, Duration.valueOf("1ms"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(0.0015D, Duration.valueOf("1ms"), false), "1");
        assertEquals(FormatUtils.formatCountRate(0.0015D, Duration.valueOf("1ms"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1ms"), false), "1000");
        assertEquals(FormatUtils.formatCountRate(1D, Duration.valueOf("1ms"), true), "1000/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ms"), false), "10K");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ms"), true), "10K/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ms"), false), "1000");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ms"), true), "1000/s");

        assertEquals(FormatUtils.formatCountRate(0.1D, Duration.valueOf("1s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(0.1D, Duration.valueOf("1s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1s"), false), "1");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1s"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("10s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("10s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(1.5D, Duration.valueOf("1s"), false), "1");
        assertEquals(FormatUtils.formatCountRate(1.5D, Duration.valueOf("1s"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1s"), false), "10");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1s"), true), "10/s");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10s"), false), "1");
        assertEquals(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10s"), true), "1/s");

        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1m"), false), "0");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1m"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(60.0D, Duration.valueOf("1m"), false), "1");
        assertEquals(FormatUtils.formatCountRate(60.0D, Duration.valueOf("1m"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(600.0D, Duration.valueOf("1m"), false), "10");
        assertEquals(FormatUtils.formatCountRate(600.0D, Duration.valueOf("1m"), true), "10/s");
        assertEquals(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10m"), false), "0");
        assertEquals(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10m"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(600.0D, Duration.valueOf("10m"), false), "1");
        assertEquals(FormatUtils.formatCountRate(600.0D, Duration.valueOf("10m"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(6000.0D, Duration.valueOf("10m"), false), "10");
        assertEquals(FormatUtils.formatCountRate(6000.0D, Duration.valueOf("10m"), true), "10/s");

        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1h"), false), "0");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1h"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(3600.0D, Duration.valueOf("1h"), false), "1");
        assertEquals(FormatUtils.formatCountRate(3600.0D, Duration.valueOf("1h"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("1h"), false), "10");
        assertEquals(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("1h"), true), "10/s");
        assertEquals(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10h"), false), "0");
        assertEquals(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10h"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("10h"), false), "1");
        assertEquals(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("10h"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(360000.0D, Duration.valueOf("10h"), false), "10");
        assertEquals(FormatUtils.formatCountRate(360000.0D, Duration.valueOf("10h"), true), "10/s");

        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1d"), false), "0");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1d"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("1d"), false), "1");
        assertEquals(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("1d"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("1d"), false), "10");
        assertEquals(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("1d"), true), "10/s");
        assertEquals(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("10d"), false), "0");
        assertEquals(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("10d"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("10d"), false), "1");
        assertEquals(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("10d"), true), "1/s");
        assertEquals(FormatUtils.formatCountRate(8640000.0D, Duration.valueOf("10d"), false), "10");
        assertEquals(FormatUtils.formatCountRate(8640000.0D, Duration.valueOf("10d"), true), "10/s");

        assertEquals(FormatUtils.formatCountRate(Double.NaN, Duration.valueOf("1s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(Double.NaN, Duration.valueOf("1s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("0s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(1.0D, Duration.valueOf("0s"), true), "0/s");
        assertEquals(FormatUtils.formatCountRate(-1.0D, Duration.valueOf("0s"), false), "0");
        assertEquals(FormatUtils.formatCountRate(-1.0D, Duration.valueOf("0s"), true), "0/s");
    }

    @Test
    public void testFormatDataSize()
    {
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1L), false), "1B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1L), true), "1B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12L), false), "12B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12L), true), "12B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123L), false), "123B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123L), true), "123B");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), false), "1.23K");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), true), "1.23KB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), false), "12.3K");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), true), "12.3KB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), false), "123K");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), true), "123KB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), false), "1.23M");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), true), "1.23MB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), false), "12.3M");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), true), "12.3MB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), false), "123M");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), true), "123MB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), false), "1.23G");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), true), "1.23GB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), false), "12.3G");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), true), "12.3GB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), false), "123G");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), true), "123GB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), false), "1.23T");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), true), "1.23TB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), false), "12.3T");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), true), "12.3TB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), false), "123T");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), true), "123TB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), false), "1.23P");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), true), "1.23PB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), false), "12.3P");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), true), "12.3PB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), false), "123P");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), true), "123PB");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), false), "1235P");
        assertEquals(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), true), "1235PB");
    }

    @Test
    public void testFormatDataRate()
    {
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), false), "1G");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), true), "1GB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), false), "10G");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), true), "10GB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), false), "1G");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), true), "1GB/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), false), "1M");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), true), "1MB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), false), "10M");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), true), "10MB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), false), "1M");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), true), "1MB/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), false), "1K");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), true), "1KB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), false), "10K");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), true), "10KB/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), false), "1K");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), true), "1KB/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), true), "10B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), true), "1B/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), true), "10B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), true), "10B/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), true), "10B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), true), "10B/s");

        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), true), "10B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), false), "0B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), true), "0B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), false), "1B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), true), "1B/s");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), false), "10B");
        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), true), "10B/s");

        // Currently, these tests fail due to https://github.com/trinodb/trino/issues/13093
//        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), false), "0B");
//        assertEquals(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), true), "0B/s");
    }

    @Test
    public void testPluralize()
    {
        assertEquals(FormatUtils.pluralize("foo", 0), "foos");
        assertEquals(FormatUtils.pluralize("foo", 1), "foo");
        assertEquals(FormatUtils.pluralize("foo", 2), "foos");
    }

    @Test
    public void testFormatFinalTime()
    {
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0us")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0ns")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0ms")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0s")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0m")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0h")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("0d")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1us")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1ns")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1ms")), "0.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("10ms")), "0.01");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("100ms")), "0.10");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1s")), "1.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("10s")), "10.00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1m")), "1:00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("61s")), "1:01");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1h")), "60:00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("61m")), "61:00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("1d")), "1440:00");
        assertEquals(FormatUtils.formatFinalTime(Duration.valueOf("25h")), "1500:00");
    }

    @Test
    public void testFormatIndeterminateProgressBar()
    {
        assertEquals(FormatUtils.formatProgressBar(10, 0), "<=>       ");
        assertEquals(FormatUtils.formatProgressBar(10, 1), " <=>      ");
        assertEquals(FormatUtils.formatProgressBar(10, 7), "       <=>");
        assertEquals(FormatUtils.formatProgressBar(10, 8), "      <=> ");
        assertEquals(FormatUtils.formatProgressBar(10, 13), " <=>      ");
        assertEquals(FormatUtils.formatProgressBar(10, 14), "<=>       ");
    }

    @Test
    public void testInvalidIndeterminateProgressBar()
    {
        IllegalArgumentException expectedException = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> FormatUtils.formatProgressBar(10, -1));
        assertEquals(expectedException.getMessage(), "invalid count: -1");
    }

    @Test
    public void testFormatProgressBar()
    {
        assertEquals(FormatUtils.formatProgressBar(10, 0, 0), "          ");
        assertEquals(FormatUtils.formatProgressBar(10, 10, 10), "=>        ");
        assertEquals(FormatUtils.formatProgressBar(10, 10, 20), "=>>       ");
        assertEquals(FormatUtils.formatProgressBar(10, 20, 10), "==>       ");
        assertEquals(FormatUtils.formatProgressBar(10, 20, 20), "==>>      ");
        assertEquals(FormatUtils.formatProgressBar(10, 50, 50), "=====>>>>>");
        assertEquals(FormatUtils.formatProgressBar(10, 100, 0), "==========");
        assertEquals(FormatUtils.formatProgressBar(10, 0, 100), ">>>>>>>>>>");
        assertEquals(FormatUtils.formatProgressBar(10, 60, 60), "======>>>>");
        assertEquals(FormatUtils.formatProgressBar(10, 120, 0), "==========");
        assertEquals(FormatUtils.formatProgressBar(10, 0, 120), ">>>>>>>>>>");
    }

    @Test
    public void testInvalidProgressBar()
    {
        IllegalArgumentException expectedException = assertThrowsExactly(
                IllegalArgumentException.class,
                () -> FormatUtils.formatProgressBar(10, -100, 0));
        assertEquals(expectedException.getMessage(), "invalid count: -9");
    }
}
