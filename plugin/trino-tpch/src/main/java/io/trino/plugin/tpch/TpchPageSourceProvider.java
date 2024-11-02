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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.tpch.TpchTable;

import java.util.List;

import static io.trino.plugin.tpch.TpchRecordSet.getRecordSet;
import static java.util.Objects.requireNonNull;

public class TpchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DecimalTypeMapping decimalTypeMapping;
    private final int maxRowsPerPage;

    @Inject
    TpchPageSourceProvider(TpchConfig config)
    {
        this(requireNonNull(config, "config is null").getMaxRowsPerPage(), config.getDecimalTypeMapping());
    }

    public TpchPageSourceProvider(int maxRowsPerPage, DecimalTypeMapping decimalTypeMapping)
    {
        this.decimalTypeMapping = requireNonNull(decimalTypeMapping, "decimalTypeMapping is null");
        this.maxRowsPerPage = maxRowsPerPage;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        TpchSplit tpchSplit = (TpchSplit) split;
        TpchTableHandle tpchTable = (TpchTableHandle) table;

        return new TpchPageSource(
                maxRowsPerPage,
                getRecordSet(
                        TpchTable.getTable(tpchTable.tableName()),
                        columns,
                        tpchTable.scaleFactor(),
                        tpchSplit.getPartNumber(),
                        tpchSplit.getTotalParts(),
                        tpchTable.constraint(),
                        decimalTypeMapping));
    }
}
