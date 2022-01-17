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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriterOptions.WriterIdentification;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestOrcWriterConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OrcWriterConfig.class)
                .setStripeMinSize(DataSize.of(32, MEGABYTE))
                .setStripeMaxSize(DataSize.of(64, MEGABYTE))
                .setStripeMaxRowCount(10_000_000)
                .setRowGroupMaxRowCount(10_000)
                .setDictionaryMaxMemory(DataSize.of(16, MEGABYTE))
                .setStringStatisticsLimit(DataSize.ofBytes(64))
                .setMaxCompressionBufferSize(DataSize.of(256, KILOBYTE))
                .setDefaultBloomFilterFpp(0.05)
                .setWriterIdentification(WriterIdentification.TRINO)
                .setValidationPercentage(0.0)
                .setValidationMode(OrcWriteValidationMode.BOTH));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.orc.writer.stripe-min-size", "13MB")
                .put("hive.orc.writer.stripe-max-size", "27MB")
                .put("hive.orc.writer.stripe-max-rows", "44")
                .put("hive.orc.writer.row-group-max-rows", "11")
                .put("hive.orc.writer.dictionary-max-memory", "13MB")
                .put("hive.orc.writer.string-statistics-limit", "17MB")
                .put("hive.orc.writer.max-compression-buffer-size", "19MB")
                .put("hive.orc.default-bloom-filter-fpp", "0.96")
                .put("hive.orc.writer.writer-identification", "LEGACY_HIVE_COMPATIBLE")
                .put("hive.orc.writer.validation-percentage", "0.16")
                .put("hive.orc.writer.validation-mode", "DETAILED")
                .buildOrThrow();

        OrcWriterConfig expected = new OrcWriterConfig()
                .setStripeMinSize(DataSize.of(13, MEGABYTE))
                .setStripeMaxSize(DataSize.of(27, MEGABYTE))
                .setStripeMaxRowCount(44)
                .setRowGroupMaxRowCount(11)
                .setDictionaryMaxMemory(DataSize.of(13, MEGABYTE))
                .setStringStatisticsLimit(DataSize.of(17, MEGABYTE))
                .setMaxCompressionBufferSize(DataSize.of(19, MEGABYTE))
                .setDefaultBloomFilterFpp(0.96)
                .setWriterIdentification(WriterIdentification.LEGACY_HIVE_COMPATIBLE)
                .setValidationPercentage(0.16)
                .setValidationMode(OrcWriteValidationMode.DETAILED);

        assertFullMapping(properties, expected);
    }
}
