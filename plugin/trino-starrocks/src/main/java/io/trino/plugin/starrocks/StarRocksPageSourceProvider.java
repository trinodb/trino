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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StarRocksPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StarRocksFlightSqlClient flightSqlClient;
    private final StarRocksArrowToPageConverter arrowToPageConverter;

    @Inject
    public StarRocksPageSourceProvider(StarRocksFlightSqlClient flightSqlClient, StarRocksArrowToPageConverter arrowToPageConverter)
    {
        this.flightSqlClient = requireNonNull(flightSqlClient, "flightSqlClient is null");
        this.arrowToPageConverter = requireNonNull(arrowToPageConverter, "arrowToPageConverter is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        StarRocksSplit starRocksSplit = (StarRocksSplit) split;
        StarRocksTableHandle starRocksTable = (StarRocksTableHandle) table;
        List<StarRocksColumnHandle> starRocksColumns = columns.stream()
                .map(StarRocksColumnHandle.class::cast)
                .toList();

        return new StarRocksFlightSqlPageSource(
                flightSqlClient.openStream(session, starRocksTable, starRocksSplit, starRocksColumns),
                arrowToPageConverter,
                starRocksColumns);
    }
}
