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
package io.trino.plugin.tpch;

import io.airlift.configuration.Config;
import io.airlift.configuration.DefunctConfig;

@DefunctConfig("tpch.produce-pages")
public class TpchConfig
{
    private ColumnNaming columnNaming = ColumnNaming.SIMPLIFIED;
    private DecimalTypeMapping decimalTypeMapping = DecimalTypeMapping.DOUBLE;
    private int maxRowsPerPage = 1_000_000;
    private String tableScanRedirectionCatalog;
    private String tableScanRedirectionSchema;
    private int splitsPerNode;
    private boolean partitioningEnabled = true;
    private boolean predicatePushdownEnabled = true;

    public ColumnNaming getColumnNaming()
    {
        return columnNaming;
    }

    @Config("tpch.column-naming")
    public TpchConfig setColumnNaming(ColumnNaming columnNaming)
    {
        this.columnNaming = columnNaming;
        return this;
    }

    public DecimalTypeMapping getDecimalTypeMapping()
    {
        return decimalTypeMapping;
    }

    @Config("tpch.double-type-mapping")
    public TpchConfig setDecimalTypeMapping(DecimalTypeMapping decimalTypeMapping)
    {
        this.decimalTypeMapping = decimalTypeMapping;
        return this;
    }

    public int getMaxRowsPerPage()
    {
        return maxRowsPerPage;
    }

    @Config("tpch.max-rows-per-page")
    public TpchConfig setMaxRowsPerPage(int maxRowsPerPage)
    {
        this.maxRowsPerPage = maxRowsPerPage;
        return this;
    }

    public String getTableScanRedirectionCatalog()
    {
        return tableScanRedirectionCatalog;
    }

    @Config("tpch.table-scan-redirection-catalog")
    public TpchConfig setTableScanRedirectionCatalog(String tableScanRedirectionCatalog)
    {
        this.tableScanRedirectionCatalog = tableScanRedirectionCatalog;
        return this;
    }

    public String getTableScanRedirectionSchema()
    {
        return tableScanRedirectionSchema;
    }

    @Config("tpch.table-scan-redirection-schema")
    public TpchConfig setTableScanRedirectionSchema(String tableScanRedirectionSchema)
    {
        this.tableScanRedirectionSchema = tableScanRedirectionSchema;
        return this;
    }

    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("tpch.splits-per-node")
    public TpchConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    public boolean isPartitioningEnabled()
    {
        return partitioningEnabled;
    }

    @Config("tpch.partitioning-enabled")
    public TpchConfig setPartitioningEnabled(boolean partitioningEnabled)
    {
        this.partitioningEnabled = partitioningEnabled;
        return this;
    }

    public boolean isPredicatePushdownEnabled()
    {
        return predicatePushdownEnabled;
    }

    @Config("tpch.predicate-pushdown-enabled")
    public TpchConfig setPredicatePushdownEnabled(boolean predicatePushdownEnabled)
    {
        this.predicatePushdownEnabled = predicatePushdownEnabled;
        return this;
    }
}
