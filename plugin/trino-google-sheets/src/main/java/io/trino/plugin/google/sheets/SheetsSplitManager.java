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
package io.trino.plugin.google.sheets;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.google.sheets.SheetsConnectorTableHandle.tableNotFound;
import static java.util.Objects.requireNonNull;

public class SheetsSplitManager
        implements ConnectorSplitManager
{
    private final SheetsClient sheetsClient;

    @Inject
    public SheetsSplitManager(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        SheetsConnectorTableHandle tableHandle = (SheetsConnectorTableHandle) connectorTableHandle;
        SheetsTable table = sheetsClient.getTable(tableHandle)
                // this can happen if table is removed during a query
                .orElseThrow(() -> tableNotFound(tableHandle));

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(sheetsSplitFromTableHandle(tableHandle, table.getValues()));
        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    private static SheetsSplit sheetsSplitFromTableHandle(
            SheetsConnectorTableHandle tableHandle,
            List<List<String>> values)
    {
        if (tableHandle instanceof SheetsNamedTableHandle namedTableHandle) {
            return new SheetsSplit(
                    Optional.of(namedTableHandle.getSchemaName()),
                    Optional.of(namedTableHandle.getTableName()),
                    Optional.empty(),
                    values);
        }
        if (tableHandle instanceof SheetsSheetTableHandle sheetTableHandle) {
            return new SheetsSplit(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(sheetTableHandle.getSheetExpression()),
                    values);
        }
        throw new IllegalStateException("Found unexpected table handle type " + tableHandle);
    }
}
