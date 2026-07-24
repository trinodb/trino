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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DorisSplitManager
        implements ConnectorSplitManager
{
    private final DorisSplitPlanner splitPlanner;

    @Inject
    public DorisSplitManager(DorisSplitPlanner splitPlanner)
    {
        this.splitPlanner = requireNonNull(splitPlanner, "splitPlanner is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;
        if (dorisTableHandle.relationType() == DorisRelationType.VIEW) {
            // Views execute as a single remote query.
            return new FixedSplitSource(List.of(new DorisSplit(
                    dorisTableHandle.remoteSchemaName(),
                    dorisTableHandle.remoteTableName(),
                    "",
                    List.of(),
                    Optional.empty())));
        }

        List<DorisSplit> splits = splitPlanner.planSplits(dorisTableHandle);
        return new FixedSplitSource(splits);
    }
}
