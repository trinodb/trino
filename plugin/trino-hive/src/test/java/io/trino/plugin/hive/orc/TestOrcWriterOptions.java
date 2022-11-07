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
package io.trino.plugin.hive.orc;

import io.airlift.units.DataSize;
import io.trino.orc.OrcWriterOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Properties;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.orc.OrcWriterOptions.WriterIdentification.LEGACY_HIVE_COMPATIBLE;
import static io.trino.orc.OrcWriterOptions.WriterIdentification.TRINO;
import static io.trino.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.trino.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.trino.plugin.hive.util.HiveUtil.getOrcWriterOptions;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestOrcWriterOptions
{
    @Test
    public void testDefaultOrcWriterOptions()
    {
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions();
        assertThat(orcWriterOptions.getWriterIdentification()).isEqualTo(TRINO);
        assertThat(orcWriterOptions.getStripeMinSize()).isEqualTo(DataSize.of(32, MEGABYTE));
        assertThat(orcWriterOptions.getStripeMaxSize()).isEqualTo(DataSize.of(64, MEGABYTE));
        assertThat(orcWriterOptions.getStripeMaxRowCount()).isEqualTo(10_000_000);
        assertThat(orcWriterOptions.getRowGroupMaxRowCount()).isEqualTo(10_000);
        assertThat(orcWriterOptions.getDictionaryMaxMemory()).isEqualTo(DataSize.of(16, MEGABYTE));
        assertThat(orcWriterOptions.getMaxStringStatisticsLimit()).isEqualTo(DataSize.ofBytes(64));
        assertThat(orcWriterOptions.getMaxCompressionBufferSize()).isEqualTo(DataSize.of(256, KILOBYTE));
        assertThat(orcWriterOptions.getBloomFilterFpp()).isEqualTo(0.05);
        assertThat(orcWriterOptions.isBloomFilterColumn("unknown_column")).isFalse();
    }

    @Test
    public void testOrcWriterOptionsFromOrcWriterConfig()
    {
        OrcWriterConfig orcWriterConfig = new OrcWriterConfig()
                .setWriterIdentification(LEGACY_HIVE_COMPATIBLE)
                .setStripeMinSize(DataSize.ofBytes(32))
                .setStripeMaxSize(DataSize.ofBytes(64))
                .setStripeMaxRowCount(100)
                .setRowGroupMaxRowCount(10)
                .setDictionaryMaxMemory(DataSize.ofBytes(16))
                .setStringStatisticsLimit(DataSize.ofBytes(16))
                .setMaxCompressionBufferSize(DataSize.ofBytes(256))
                .setDefaultBloomFilterFpp(0.5);
        OrcWriterOptions orcWriterOptions = orcWriterConfig.toOrcWriterOptions();
        assertThat(orcWriterOptions.getWriterIdentification()).isEqualTo(LEGACY_HIVE_COMPATIBLE);
        assertThat(orcWriterOptions.getStripeMinSize()).isEqualTo(DataSize.ofBytes(32));
        assertThat(orcWriterOptions.getStripeMaxSize()).isEqualTo(DataSize.ofBytes(64));
        assertThat(orcWriterOptions.getStripeMaxRowCount()).isEqualTo(100);
        assertThat(orcWriterOptions.getRowGroupMaxRowCount()).isEqualTo(10);
        assertThat(orcWriterOptions.getDictionaryMaxMemory()).isEqualTo(DataSize.ofBytes(16));
        assertThat(orcWriterOptions.getMaxStringStatisticsLimit()).isEqualTo(DataSize.ofBytes(16));
        assertThat(orcWriterOptions.getMaxCompressionBufferSize()).isEqualTo(DataSize.ofBytes(256));
        assertThat(orcWriterOptions.getBloomFilterFpp()).isEqualTo(0.5);
        assertThat(orcWriterOptions.isBloomFilterColumn("unknown_column")).isFalse();
    }

    @Test
    public void testOrcWriterOptionsFromTableProperties()
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "column_a, column_b");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, "0.5");
        OrcWriterOptions orcWriterOptions = getOrcWriterOptions(tableProperties, new OrcWriterOptions());
        assertThat(orcWriterOptions.getBloomFilterFpp()).isEqualTo(0.5);
        assertThat(orcWriterOptions.isBloomFilterColumn("column_a")).isTrue();
        assertThat(orcWriterOptions.isBloomFilterColumn("column_b")).isTrue();
        assertThat(orcWriterOptions.isBloomFilterColumn("unknown_column")).isFalse();
    }

    @Test
    public void testOrcWriterOptionsWithInvalidFPPValue()
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "column_with_bloom_filter");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, "abc");
        assertThatThrownBy(() -> getOrcWriterOptions(tableProperties, new OrcWriterOptions()))
                .hasMessage("Invalid value for orc_bloom_filter_fpp property: abc");
    }

    @Test(dataProvider = "invalidBloomFilterFpp")
    public void testOrcBloomFilterWithInvalidRange(String fpp)
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "column_with_bloom_filter");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, fpp);
        assertThatThrownBy(() -> getOrcWriterOptions(tableProperties, new OrcWriterOptions()))
                .hasMessage("bloomFilterFpp should be > 0.0 & < 1.0");
    }

    @DataProvider
    public Object[][] invalidBloomFilterFpp()
    {
        return new Object[][]{
                {"100"},
                {"-100"},
                {"0"},
                {"1"}
        };
    }
}
