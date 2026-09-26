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
package io.trino.plugin.starrocks;

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

import static java.util.Objects.requireNonNull;

public class StarRocksSplitManager
        implements ConnectorSplitManager
{
    private final StarRocksFlightSqlClient flightSqlClient;

    @Inject
    public StarRocksSplitManager(StarRocksFlightSqlClient flightSqlClient)
    {
        this.flightSqlClient = requireNonNull(flightSqlClient, "flightSqlClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        StarRocksTableHandle starRocksTable = (StarRocksTableHandle) tableHandle;
        if (starRocksTable.projectedColumns().isEmpty()) {
            return new FixedSplitSource(List.of(new StarRocksSplit()));
        }
        return new FixedSplitSource(flightSqlClient.getSplits(session, starRocksTable, starRocksTable.projectedColumns().orElseThrow()));
    }
}
