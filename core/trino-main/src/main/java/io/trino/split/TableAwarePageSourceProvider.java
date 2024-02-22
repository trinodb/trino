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
package io.trino.split;

import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorContext;
import io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSourceProvider;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativePageSourceProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableAwarePageSourceProvider
        implements Closeable
{
    private final PageSourceProvider pageSourceProvider;
    private final TableHandle tableHandle;
    private final Optional<ConnectorAlternativePageSourceProvider> connectorAlternativePageSourceProvider;
    private final DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider;

    public static TableAwarePageSourceProvider create(
            OperatorContext operatorContext,
            TableHandle table,
            PageSourceProvider pageSourceProvider,
            DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider)
    {
        return new TableAwarePageSourceProvider(
                pageSourceProvider,
                table,
                operatorContext.getDriverContext().getConnectorAlternativePageSourceProvider(),
                dynamicRowFilteringPageSourceProvider);
    }

    private TableAwarePageSourceProvider(
            PageSourceProvider pageSourceProvider,
            TableHandle tableHandle,
            Optional<ConnectorAlternativePageSourceProvider> connectorAlternativePageSourceProvider,
            DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.connectorAlternativePageSourceProvider = requireNonNull(connectorAlternativePageSourceProvider, "connectorAlternativePageSourceProvider is null");
        this.dynamicRowFilteringPageSourceProvider = requireNonNull(dynamicRowFilteringPageSourceProvider, "dynamicRowFilteringPageSourceProvider is null");
    }

    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        if (split.getConnectorSplit() instanceof EmptySplit) {
            return new EmptyPageSource();
        }
        return connectorAlternativePageSourceProvider
                .map(factory -> {
                    ConnectorPageSource pageSource = factory.createPageSource(
                            tableHandle.getTransaction(),
                            session.toConnectorSession(tableHandle.getCatalogHandle()),
                            columns,
                            dynamicFilter,
                            !split.getFailoverHappened());
                    return dynamicRowFilteringPageSourceProvider.createPageSource(pageSource, session, columns, dynamicFilter);
                })
                .orElseGet(() -> pageSourceProvider.createPageSource(session, split, tableHandle, columns, dynamicFilter));
    }

    @Override
    public void close()
    {
        connectorAlternativePageSourceProvider.ifPresent(ConnectorAlternativePageSourceProvider::close);
    }
}
