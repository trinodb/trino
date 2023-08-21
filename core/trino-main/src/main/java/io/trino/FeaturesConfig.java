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
package io.trino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.trino.sql.analyzer.RegexLibrary;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.sql.analyzer.RegexLibrary.JONI;

@DefunctConfig({
        "analyzer.experimental-syntax-enabled",
        "arrayagg.implementation",
        "deprecated.disable-set-properties-security-check-for-create-ddl",
        "deprecated.group-by-uses-equal",
        "deprecated.legacy-char-to-varchar-coercion",
        "deprecated.legacy-join-using",
        "deprecated.legacy-map-subscript",
        "deprecated.legacy-order-by",
        "deprecated.legacy-row-field-ordinal-access",
        "deprecated.legacy-row-to-json-cast",
        "deprecated.legacy-timestamp",
        "deprecated.legacy-unnest-array-rows",
        "deprecated.legacy-update-delete-implementation",
        "experimental-syntax-enabled",
        "experimental.resource-groups-enabled",
        "fast-inequality-joins",
        "histogram.implementation",
        "multimapagg.implementation",
        "optimizer.iterative-rule-based-column-pruning",
        "optimizer.processing-optimization",
        "resource-group-manager",
        "spill-order-by",
        "experimental.spill-order-by",
        "spill-window-operator",
        "experimental.spill-window-operator",
        "legacy.allow-set-view-authorization",
})
public class FeaturesConfig
{
    @VisibleForTesting
    public static final String SPILLER_SPILL_PATH = "spiller-spill-path";

    private boolean redistributeWrites = true;
    private boolean scaleWriters = true;
    private DataSize writerScalingMinDataProcessed = DataSize.of(120, DataSize.Unit.MEGABYTE);
    private DataIntegrityVerification exchangeDataIntegrityVerification = DataIntegrityVerification.ABORT;
    /**
     * default value is overwritten for fault tolerant execution in {@link #applyFaultTolerantExecutionDefaults()}}
     */
    private boolean exchangeCompressionEnabled;
    private boolean pagesIndexEagerCompactionEnabled;
    private boolean omitDateTimeTypePrecision;
    private int maxRecursionDepth = 10;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private boolean spillEnabled;
    private DataSize aggregationOperatorUnspillMemoryLimit = DataSize.of(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of();
    private int spillerThreads = 4;
    private double spillMaxUsedSpaceThreshold = 0.9;
    private double memoryRevokingTarget = 0.5;
    private double memoryRevokingThreshold = 0.9;
    private boolean parseDecimalLiteralsAsDouble;
    private boolean lateMaterializationEnabled;

    private DataSize filterAndProjectMinOutputPageSize = DataSize.of(500, KILOBYTE);
    private int filterAndProjectMinOutputPageRowCount = 256;
    private int maxGroupingSets = 2048;

    private boolean legacyCatalogRoles;
    private boolean incrementalHashArrayLoadFactorEnabled = true;

    private boolean legacyMaterializedViewGracePeriod;
    private boolean hideInaccessibleColumns;
    private boolean forceSpillingJoin;

    private boolean faultTolerantExecutionExchangeEncryptionEnabled = true;

    private boolean flatGroupByHash = true;

    public enum DataIntegrityVerification
    {
        NONE,
        ABORT,
        RETRY,
        /**/;
    }

    public boolean isOmitDateTimeTypePrecision()
    {
        return omitDateTimeTypePrecision;
    }

    @Config("deprecated.omit-datetime-type-precision")
    @ConfigDescription("Enable compatibility mode for legacy clients when rendering datetime type names with default precision")
    public FeaturesConfig setOmitDateTimeTypePrecision(boolean value)
    {
        this.omitDateTimeTypePrecision = value;
        return this;
    }

    public boolean isRedistributeWrites()
    {
        return redistributeWrites;
    }

    @Config("redistribute-writes")
    public FeaturesConfig setRedistributeWrites(boolean redistributeWrites)
    {
        this.redistributeWrites = redistributeWrites;
        return this;
    }

    public boolean isScaleWriters()
    {
        return scaleWriters;
    }

    @Config("scale-writers")
    public FeaturesConfig setScaleWriters(boolean scaleWriters)
    {
        this.scaleWriters = scaleWriters;
        return this;
    }

    @NotNull
    public DataSize getWriterScalingMinDataProcessed()
    {
        return writerScalingMinDataProcessed;
    }

    @Config("writer-scaling-min-data-processed")
    @ConfigDescription("Minimum amount of uncompressed output data processed by writers before writer scaling can happen")
    public FeaturesConfig setWriterScalingMinDataProcessed(DataSize writerScalingMinDataProcessed)
    {
        this.writerScalingMinDataProcessed = writerScalingMinDataProcessed;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "writer-min-size", replacedBy = "writer-scaling-min-data-processed")
    @ConfigDescription("Target minimum size of writer output when scaling writers")
    public FeaturesConfig setWriterMinSize(DataSize writerMinSize)
    {
        this.writerScalingMinDataProcessed = succinctBytes(writerMinSize.toBytes() * 2);
        return this;
    }

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-states-limit")
    public FeaturesConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("re2j.dfa-retries")
    public FeaturesConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("regex-library")
    public FeaturesConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public boolean isSpillEnabled()
    {
        return spillEnabled;
    }

    @Config("spill-enabled")
    @LegacyConfig("experimental.spill-enabled")
    public FeaturesConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public DataSize getAggregationOperatorUnspillMemoryLimit()
    {
        return aggregationOperatorUnspillMemoryLimit;
    }

    @Config("aggregation-operator-unspill-memory-limit")
    @LegacyConfig("experimental.aggregation-operator-unspill-memory-limit")
    public FeaturesConfig setAggregationOperatorUnspillMemoryLimit(DataSize aggregationOperatorUnspillMemoryLimit)
    {
        this.aggregationOperatorUnspillMemoryLimit = aggregationOperatorUnspillMemoryLimit;
        return this;
    }

    public List<Path> getSpillerSpillPaths()
    {
        return spillerSpillPaths;
    }

    @Config("spiller-spill-path")
    @LegacyConfig("experimental.spiller-spill-path")
    public FeaturesConfig setSpillerSpillPaths(String spillPaths)
    {
        List<String> spillPathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(spillPaths));
        this.spillerSpillPaths = spillPathsSplit.stream().map(Paths::get).collect(toImmutableList());
        return this;
    }

    @Min(1)
    public int getSpillerThreads()
    {
        return spillerThreads;
    }

    @Config("spiller-threads")
    @LegacyConfig("experimental.spiller-threads")
    public FeaturesConfig setSpillerThreads(int spillerThreads)
    {
        this.spillerThreads = spillerThreads;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingThreshold()
    {
        return memoryRevokingThreshold;
    }

    @Config("memory-revoking-threshold")
    @LegacyConfig("experimental.memory-revoking-threshold")
    @ConfigDescription("Revoke memory when memory pool is filled over threshold")
    public FeaturesConfig setMemoryRevokingThreshold(double memoryRevokingThreshold)
    {
        this.memoryRevokingThreshold = memoryRevokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingTarget()
    {
        return memoryRevokingTarget;
    }

    @Config("memory-revoking-target")
    @LegacyConfig("experimental.memory-revoking-target")
    @ConfigDescription("When revoking memory, try to revoke so much that pool is filled below target at the end")
    public FeaturesConfig setMemoryRevokingTarget(double memoryRevokingTarget)
    {
        this.memoryRevokingTarget = memoryRevokingTarget;
        return this;
    }

    public double getSpillMaxUsedSpaceThreshold()
    {
        return spillMaxUsedSpaceThreshold;
    }

    @Config("spiller-max-used-space-threshold")
    @LegacyConfig("experimental.spiller-max-used-space-threshold")
    public FeaturesConfig setSpillMaxUsedSpaceThreshold(double spillMaxUsedSpaceThreshold)
    {
        this.spillMaxUsedSpaceThreshold = spillMaxUsedSpaceThreshold;
        return this;
    }

    public boolean isExchangeCompressionEnabled()
    {
        return exchangeCompressionEnabled;
    }

    @Config("exchange.compression-enabled")
    public FeaturesConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }

    public DataIntegrityVerification getExchangeDataIntegrityVerification()
    {
        return exchangeDataIntegrityVerification;
    }

    @Config("exchange.data-integrity-verification")
    public FeaturesConfig setExchangeDataIntegrityVerification(DataIntegrityVerification exchangeDataIntegrityVerification)
    {
        this.exchangeDataIntegrityVerification = exchangeDataIntegrityVerification;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("parse-decimal-literals-as-double")
    public FeaturesConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    public boolean isPagesIndexEagerCompactionEnabled()
    {
        return pagesIndexEagerCompactionEnabled;
    }

    @Config("pages-index.eager-compaction-enabled")
    public FeaturesConfig setPagesIndexEagerCompactionEnabled(boolean pagesIndexEagerCompactionEnabled)
    {
        this.pagesIndexEagerCompactionEnabled = pagesIndexEagerCompactionEnabled;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getFilterAndProjectMinOutputPageSize()
    {
        return filterAndProjectMinOutputPageSize;
    }

    @Config("filter-and-project-min-output-page-size")
    @LegacyConfig("experimental.filter-and-project-min-output-page-size")
    public FeaturesConfig setFilterAndProjectMinOutputPageSize(DataSize filterAndProjectMinOutputPageSize)
    {
        this.filterAndProjectMinOutputPageSize = filterAndProjectMinOutputPageSize;
        return this;
    }

    @Min(0)
    public int getFilterAndProjectMinOutputPageRowCount()
    {
        return filterAndProjectMinOutputPageRowCount;
    }

    @Config("filter-and-project-min-output-page-row-count")
    @LegacyConfig("experimental.filter-and-project-min-output-page-row-count")
    public FeaturesConfig setFilterAndProjectMinOutputPageRowCount(int filterAndProjectMinOutputPageRowCount)
    {
        this.filterAndProjectMinOutputPageRowCount = filterAndProjectMinOutputPageRowCount;
        return this;
    }

    public int getMaxRecursionDepth()
    {
        return maxRecursionDepth;
    }

    @Config("max-recursion-depth")
    @ConfigDescription("Maximum recursion depth for recursive common table expression")
    public FeaturesConfig setMaxRecursionDepth(int maxRecursionDepth)
    {
        this.maxRecursionDepth = maxRecursionDepth;
        return this;
    }

    public int getMaxGroupingSets()
    {
        return maxGroupingSets;
    }

    @Config("analyzer.max-grouping-sets")
    public FeaturesConfig setMaxGroupingSets(int maxGroupingSets)
    {
        this.maxGroupingSets = maxGroupingSets;
        return this;
    }

    public boolean isLateMaterializationEnabled()
    {
        return lateMaterializationEnabled;
    }

    @Config("experimental.late-materialization.enabled")
    @LegacyConfig("experimental.work-processor-pipelines")
    public FeaturesConfig setLateMaterializationEnabled(boolean lateMaterializationEnabled)
    {
        this.lateMaterializationEnabled = lateMaterializationEnabled;
        return this;
    }

    public boolean isLegacyCatalogRoles()
    {
        return legacyCatalogRoles;
    }

    @Config("deprecated.legacy-catalog-roles")
    @ConfigDescription("Enable legacy role management syntax that assumed all roles are catalog scoped")
    public FeaturesConfig setLegacyCatalogRoles(boolean legacyCatalogRoles)
    {
        this.legacyCatalogRoles = legacyCatalogRoles;
        return this;
    }

    @Deprecated
    public boolean isIncrementalHashArrayLoadFactorEnabled()
    {
        return incrementalHashArrayLoadFactorEnabled;
    }

    @Deprecated
    @Config("incremental-hash-array-load-factor.enabled")
    @ConfigDescription("Use smaller load factor for small hash arrays in order to improve performance")
    public FeaturesConfig setIncrementalHashArrayLoadFactorEnabled(boolean incrementalHashArrayLoadFactorEnabled)
    {
        this.incrementalHashArrayLoadFactorEnabled = incrementalHashArrayLoadFactorEnabled;
        return this;
    }

    @Deprecated
    public boolean isLegacyMaterializedViewGracePeriod()
    {
        return legacyMaterializedViewGracePeriod;
    }

    @Deprecated
    @Config("legacy.materialized-view-grace-period")
    @ConfigDescription("Enable legacy handling of stale materialized views")
    public FeaturesConfig setLegacyMaterializedViewGracePeriod(boolean legacyMaterializedViewGracePeriod)
    {
        this.legacyMaterializedViewGracePeriod = legacyMaterializedViewGracePeriod;
        return this;
    }

    public boolean isHideInaccessibleColumns()
    {
        return hideInaccessibleColumns;
    }

    @Config("hide-inaccessible-columns")
    @ConfigDescription("When enabled non-accessible columns are silently filtered from results from SELECT * statements")
    public FeaturesConfig setHideInaccessibleColumns(boolean hideInaccessibleColumns)
    {
        this.hideInaccessibleColumns = hideInaccessibleColumns;
        return this;
    }

    public boolean isForceSpillingJoin()
    {
        return forceSpillingJoin;
    }

    @Config("force-spilling-join-operator")
    @ConfigDescription("Force spilling join operator in favour of the non-spilling one even when there is no spill")
    public FeaturesConfig setForceSpillingJoin(boolean forceSpillingJoin)
    {
        this.forceSpillingJoin = forceSpillingJoin;
        return this;
    }

    public boolean isFaultTolerantExecutionExchangeEncryptionEnabled()
    {
        return faultTolerantExecutionExchangeEncryptionEnabled;
    }

    @Config("fault-tolerant-execution.exchange-encryption-enabled")
    public FeaturesConfig setFaultTolerantExecutionExchangeEncryptionEnabled(boolean faultTolerantExecutionExchangeEncryptionEnabled)
    {
        this.faultTolerantExecutionExchangeEncryptionEnabled = faultTolerantExecutionExchangeEncryptionEnabled;
        return this;
    }

    public void applyFaultTolerantExecutionDefaults()
    {
        exchangeCompressionEnabled = true;
    }

    @Deprecated
    public boolean isFlatGroupByHash()
    {
        return flatGroupByHash;
    }

    @Deprecated
    @Config("legacy.flat-group-by-hash")
    public FeaturesConfig setFlatGroupByHash(boolean flatGroupByHash)
    {
        this.flatGroupByHash = flatGroupByHash;
        return this;
    }
}
