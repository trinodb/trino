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
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorAlternativePageSourceProvider;
import io.trino.spi.connector.ConnectorSession;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AlternativeChooser
{
    private final CatalogServiceProvider<ConnectorAlternativeChooser> alternativeChooserProvider;

    @Inject
    public AlternativeChooser(CatalogServiceProvider<ConnectorAlternativeChooser> alternativeChooserProvider)
    {
        this.alternativeChooserProvider = requireNonNull(alternativeChooserProvider, "alternativeChooserProvider is null");
    }

    public Choice chooseAlternative(Session session, Split split, Collection<TableHandle> alternatives)
    {
        if (split.getConnectorSplit() instanceof EmptySplit) {
            return new Choice(alternatives.iterator().next(), (transaction, session1, columns, dynamicFilter, splitAddressEnforced) -> {
                throw new UnsupportedOperationException("Cannot create page source for empty split");
            });
        }
        CatalogHandle catalogHandle = split.getCatalogHandle();
        ConnectorAlternativeChooser alternativeChooser = alternativeChooserProvider.getService(catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        List<TableHandle> orderedAlternatives = ImmutableList.copyOf(alternatives);
        ConnectorAlternativeChooser.Choice choice = alternativeChooser.chooseAlternative(
                connectorSession,
                split.getConnectorSplit(),
                orderedAlternatives.stream().map(TableHandle::getConnectorHandle).collect(toImmutableList()));
        return new Choice(orderedAlternatives.get(choice.chosenTableHandleIndex()), choice.pageSourceProvider());
    }

    public record Choice(TableHandle tableHandle, ConnectorAlternativePageSourceProvider pageSourceProvider)
    {
        public Choice
        {
            requireNonNull(tableHandle, "tableHandle is null");
            requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        }
    }
}
