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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.execution.buffer.CompressionCodec.ZSTD;
import static io.trino.sql.analyzer.RegexLibrary.JONI;
import static io.trino.sql.analyzer.RegexLibrary.RE2J;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FeaturesConfig.class)
                .setRedistributeWrites(true)
                .setScaleWriters(true)
                .setWriterScalingMinDataProcessed(DataSize.of(120, MEGABYTE))
                .setMaxMemoryPerPartitionWriter(DataSize.of(256, MEGABYTE))
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths("")
                .setSpillerThreads(4)
                .setSpillMaxUsedSpaceThreshold(0.9)
                .setMemoryRevokingThreshold(0.9)
                .setMemoryRevokingTarget(0.5)
                .setExchangeCompressionCodec(NONE)
                .setExchangeDataIntegrityVerification(DataIntegrityVerification.ABORT)
                .setPagesIndexEagerCompactionEnabled(false)
                .setFilterAndProjectMinOutputPageSize(DataSize.of(500, KILOBYTE))
                .setFilterAndProjectMinOutputPageRowCount(256)
                .setMaxRecursionDepth(10)
                .setMaxGroupingSets(2048)
                .setOmitDateTimeTypePrecision(false)
                .setLegacyCatalogRoles(false)
                .setIncrementalHashArrayLoadFactorEnabled(true)
                .setHideInaccessibleColumns(false)
                .setForceSpillingJoin(false)
                .setFaultTolerantExecutionExchangeEncryptionEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redistribute-writes", "false")
                .put("scale-writers", "false")
                .put("writer-scaling-min-data-processed", "4GB")
                .put("max-memory-per-partition-writer", "4GB")
                .put("deprecated.regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("spill-enabled", "true")
                .put("aggregation-operator-unspill-memory-limit", "100MB")
                .put("spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("spiller-threads", "42")
                .put("spiller-max-used-space-threshold", "0.8")
                .put("memory-revoking-threshold", "0.2")
                .put("memory-revoking-target", "0.8")
                .put("exchange.compression-codec", "ZSTD")
                .put("exchange.data-integrity-verification", "RETRY")
                .put("pages-index.eager-compaction-enabled", "true")
                .put("filter-and-project-min-output-page-size", "1MB")
                .put("filter-and-project-min-output-page-row-count", "2048")
                .put("max-recursion-depth", "8")
                .put("analyzer.max-grouping-sets", "2047")
                .put("deprecated.omit-datetime-type-precision", "true")
                .put("deprecated.legacy-catalog-roles", "true")
                .put("incremental-hash-array-load-factor.enabled", "false")
                .put("hide-inaccessible-columns", "true")
                .put("force-spilling-join-operator", "true")
                .put("fault-tolerant-execution.exchange-encryption-enabled", "false")
                .buildOrThrow();

        FeaturesConfig expected = new FeaturesConfig()
                .setRedistributeWrites(false)
                .setScaleWriters(false)
                .setWriterScalingMinDataProcessed(DataSize.of(4, GIGABYTE))
                .setMaxMemoryPerPartitionWriter(DataSize.of(4, GIGABYTE))
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .setSpillerThreads(42)
                .setSpillMaxUsedSpaceThreshold(0.8)
                .setMemoryRevokingThreshold(0.2)
                .setMemoryRevokingTarget(0.8)
                .setExchangeCompressionCodec(ZSTD)
                .setExchangeDataIntegrityVerification(DataIntegrityVerification.RETRY)
                .setPagesIndexEagerCompactionEnabled(true)
                .setFilterAndProjectMinOutputPageSize(DataSize.of(1, MEGABYTE))
                .setFilterAndProjectMinOutputPageRowCount(2048)
                .setMaxRecursionDepth(8)
                .setMaxGroupingSets(2047)
                .setOmitDateTimeTypePrecision(true)
                .setLegacyCatalogRoles(true)
                .setIncrementalHashArrayLoadFactorEnabled(false)
                .setHideInaccessibleColumns(true)
                .setForceSpillingJoin(true)
                .setFaultTolerantExecutionExchangeEncryptionEnabled(false);
        assertFullMapping(properties, expected);
    }
}
