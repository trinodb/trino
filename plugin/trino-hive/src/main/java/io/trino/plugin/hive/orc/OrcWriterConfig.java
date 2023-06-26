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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterOptions.WriterIdentification;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

@DefunctConfig("hive.orc.optimized-writer.enabled")
@SuppressWarnings("unused")
public class OrcWriterConfig
{
    private OrcWriterOptions options = new OrcWriterOptions();

    private double defaultBloomFilterFpp = 0.05;
    private double validationPercentage;
    private OrcWriteValidationMode validationMode = OrcWriteValidationMode.BOTH;

    public OrcWriterOptions toOrcWriterOptions()
    {
        return options;
    }

    public DataSize getStripeMinSize()
    {
        return options.getStripeMinSize();
    }

    @Config("hive.orc.writer.stripe-min-size")
    public OrcWriterConfig setStripeMinSize(DataSize stripeMinSize)
    {
        options = options.withStripeMinSize(stripeMinSize);
        return this;
    }

    public DataSize getStripeMaxSize()
    {
        return options.getStripeMaxSize();
    }

    @Config("hive.orc.writer.stripe-max-size")
    public OrcWriterConfig setStripeMaxSize(DataSize stripeMaxSize)
    {
        options = options.withStripeMaxSize(stripeMaxSize);
        return this;
    }

    public int getStripeMaxRowCount()
    {
        return options.getStripeMaxRowCount();
    }

    @Config("hive.orc.writer.stripe-max-rows")
    public OrcWriterConfig setStripeMaxRowCount(int stripeMaxRowCount)
    {
        options = options.withStripeMaxRowCount(stripeMaxRowCount);
        return this;
    }

    public int getRowGroupMaxRowCount()
    {
        return options.getRowGroupMaxRowCount();
    }

    @Config("hive.orc.writer.row-group-max-rows")
    public OrcWriterConfig setRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        options = options.withRowGroupMaxRowCount(rowGroupMaxRowCount);
        return this;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return options.getDictionaryMaxMemory();
    }

    @Config("hive.orc.writer.dictionary-max-memory")
    public OrcWriterConfig setDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        options = options.withDictionaryMaxMemory(dictionaryMaxMemory);
        return this;
    }

    public DataSize getStringStatisticsLimit()
    {
        return options.getMaxStringStatisticsLimit();
    }

    @Config("hive.orc.writer.string-statistics-limit")
    public OrcWriterConfig setStringStatisticsLimit(DataSize stringStatisticsLimit)
    {
        options = options.withMaxStringStatisticsLimit(stringStatisticsLimit);
        return this;
    }

    public DataSize getMaxCompressionBufferSize()
    {
        return options.getMaxCompressionBufferSize();
    }

    @Config("hive.orc.writer.max-compression-buffer-size")
    public OrcWriterConfig setMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
    {
        options = options.withMaxCompressionBufferSize(maxCompressionBufferSize);
        return this;
    }

    public double getDefaultBloomFilterFpp()
    {
        return options.getBloomFilterFpp();
    }

    @Config("hive.orc.default-bloom-filter-fpp")
    @ConfigDescription("ORC Bloom filter false positive probability")
    public OrcWriterConfig setDefaultBloomFilterFpp(double defaultBloomFilterFpp)
    {
        options = options.withBloomFilterFpp(defaultBloomFilterFpp);
        return this;
    }

    @Deprecated
    public boolean isUseLegacyVersion()
    {
        return options.getWriterIdentification() == WriterIdentification.LEGACY_HIVE_COMPATIBLE;
    }

    @Deprecated
    @LegacyConfig(value = "hive.orc.writer.use-legacy-version-number", replacedBy = "hive.orc.writer.writer-identification")
    @ConfigDescription("Write ORC files with a version number that is readable by Hive 2.0.0 to 2.2.0")
    public OrcWriterConfig setUseLegacyVersion(boolean useLegacyVersion)
    {
        this.options = options.withWriterIdentification(useLegacyVersion ? WriterIdentification.LEGACY_HIVE_COMPATIBLE : WriterIdentification.TRINO);
        return this;
    }

    @NotNull
    public WriterIdentification getWriterIdentification()
    {
        return options.getWriterIdentification();
    }

    @Config("hive.orc.writer.writer-identification")
    public OrcWriterConfig setWriterIdentification(WriterIdentification writerIdentification)
    {
        options = options.withWriterIdentification(writerIdentification);
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getValidationPercentage()
    {
        return validationPercentage;
    }

    @Config("hive.orc.writer.validation-percentage")
    @ConfigDescription("Percentage of ORC files to validate after write by re-reading the whole file")
    public OrcWriterConfig setValidationPercentage(double validationPercentage)
    {
        this.validationPercentage = validationPercentage;
        return this;
    }

    @NotNull
    public OrcWriteValidationMode getValidationMode()
    {
        return validationMode;
    }

    @Config("hive.orc.writer.validation-mode")
    @ConfigDescription("Level of detail in ORC validation. Lower levels require more memory.")
    public OrcWriterConfig setValidationMode(OrcWriteValidationMode validationMode)
    {
        this.validationMode = validationMode;
        return this;
    }
}
