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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorAlternativePageSourceProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * {@link ConnectorPageSourceProvider} is required for case where connector implements only {@link ConnectorAlternativeChooser},
 * but the plan does not use alternatives.
 * See {@link TableAwarePageSourceProvider#createPageSource}.
 */
public class AlternativeChooserPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorAlternativeChooser alternativeChooser;

    public AlternativeChooserPageSourceProvider(ConnectorAlternativeChooser alternativeChooser)
    {
        this.alternativeChooser = requireNonNull(alternativeChooser, "alternativeChooser is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        return createPageSource(transaction, session, split, table, columns, dynamicFilter, true);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            boolean splitAddressEnforced)
    {
        ConnectorAlternativeChooser.Choice choice = alternativeChooser.chooseAlternative(session, split, ImmutableList.of(table));
        try (ConnectorAlternativePageSourceProvider provider = choice.pageSourceProvider()) {
            return provider.createPageSource(transaction, session, columns, dynamicFilter, splitAddressEnforced);
        }
    }

    @Override
    public TupleDomain<ColumnHandle> simplifyPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        return alternativeChooser.simplifyPredicate(session, split, table, predicate);
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        return alternativeChooser.prunePredicate(session, split, table, predicate);
    }

    @Override
    public boolean shouldPerformDynamicRowFiltering()
    {
        return alternativeChooser.shouldPerformDynamicRowFiltering();
    }
}
