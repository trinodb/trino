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
package io.trino.plugin.deltalake.delegating;

import io.trino.plugin.deltalake.DeltaLakeSplitManager;
import io.trino.plugin.deltalake.kernel.KernelDeltaLakeSplitManager;
import io.trino.plugin.deltalake.kernel.KernelDeltaLakeTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import jakarta.inject.Inject;

public class DelegatingSplitManager
        implements ConnectorSplitManager
{
    private final DeltaLakeSplitManager deltaLakeSplitManager;
    private final KernelDeltaLakeSplitManager kernelDeltaLakeSplitManager;

    @Inject
    public DelegatingSplitManager(
            DeltaLakeSplitManager deltaLakeSplitManager,
            KernelDeltaLakeSplitManager kernelDeltaLakeSplitManager)
    {
        this.deltaLakeSplitManager = deltaLakeSplitManager;
        this.kernelDeltaLakeSplitManager = kernelDeltaLakeSplitManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof KernelDeltaLakeTableHandle) {
            return kernelDeltaLakeSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        return deltaLakeSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
    {
        // Kernel has no support for connector table functions yet
        return deltaLakeSplitManager.getSplits(transaction, session, function);
    }
}
