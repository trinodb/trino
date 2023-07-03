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

import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    public static final String TPCH_COLUMN_NAMING_PROPERTY = "tpch.column-naming";
    public static final String TPCH_DOUBLE_TYPE_MAPPING_PROPERTY = "tpch.double-type-mapping";
    public static final String TPCH_PRODUCE_PAGES = "tpch.produce-pages";
    public static final String TPCH_MAX_ROWS_PER_PAGE_PROPERTY = "tpch.max-rows-per-page";
    public static final String TPCH_TABLE_SCAN_REDIRECTION_CATALOG = "tpch.table-scan-redirection-catalog";
    public static final String TPCH_TABLE_SCAN_REDIRECTION_SCHEMA = "tpch.table-scan-redirection-schema";
    public static final String TPCH_SPLITS_PER_NODE = "tpch.splits-per-node";
    public static final String TPCH_PARTITIONING_ENABLED = "tpch.partitioning-enabled";
    private static final int DEFAULT_MAX_ROWS_PER_PAGE = 1_000_000;

    private final int defaultSplitsPerNode;
    private final boolean predicatePushdownEnabled;

    public TpchConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TpchConnectorFactory(int defaultSplitsPerNode)
    {
        this(defaultSplitsPerNode, true);
    }

    public TpchConnectorFactory(int defaultSplitsPerNode, boolean predicatePushdownEnabled)
    {
        this.defaultSplitsPerNode = defaultSplitsPerNode;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
    }

    @Override
    public String getName()
    {
        return "tpch";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        int splitsPerNode = getSplitsPerNode(properties);
        ColumnNaming columnNaming = ColumnNaming.valueOf(properties.getOrDefault(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()).toUpperCase(ENGLISH));
        DecimalTypeMapping decimalTypeMapping = DecimalTypeMapping.valueOf(properties.getOrDefault(TPCH_DOUBLE_TYPE_MAPPING_PROPERTY, DecimalTypeMapping.DOUBLE.name()).toUpperCase(ENGLISH));
        boolean partitioningEnabled = Boolean.parseBoolean(properties.getOrDefault(TPCH_PARTITIONING_ENABLED, "true"));
        NodeManager nodeManager = context.getNodeManager();

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
            {
                return TpchTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
            {
                return new TpchMetadata(
                        columnNaming,
                        decimalTypeMapping,
                        predicatePushdownEnabled,
                        partitioningEnabled,
                        getTpchTableScanRedirectionCatalog(properties),
                        getTpchTableScanRedirectionSchema(properties));
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorPageSourceProvider getPageSourceProvider()
            {
                if (isProducePages(properties)) {
                    return new TpchPageSourceProvider(getMaxRowsPerPage(properties), decimalTypeMapping);
                }

                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                if (!isProducePages(properties)) {
                    return new TpchRecordSetProvider(decimalTypeMapping);
                }

                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get(TPCH_SPLITS_PER_NODE), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property " + TPCH_SPLITS_PER_NODE);
        }
    }

    private boolean isProducePages(Map<String, String> properties)
    {
        return Boolean.parseBoolean(firstNonNull(properties.get(TPCH_PRODUCE_PAGES), FALSE.toString()));
    }

    private int getMaxRowsPerPage(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get(TPCH_MAX_ROWS_PER_PAGE_PROPERTY), String.valueOf(DEFAULT_MAX_ROWS_PER_PAGE)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("Invalid property %s", TPCH_MAX_ROWS_PER_PAGE_PROPERTY));
        }
    }

    private Optional<String> getTpchTableScanRedirectionCatalog(Map<String, String> properties)
    {
        return Optional.ofNullable(properties.get(TPCH_TABLE_SCAN_REDIRECTION_CATALOG));
    }

    private Optional<String> getTpchTableScanRedirectionSchema(Map<String, String> properties)
    {
        return Optional.ofNullable(properties.get(TPCH_TABLE_SCAN_REDIRECTION_SCHEMA));
    }
}
