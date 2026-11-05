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
package io.trino.plugin.faker;

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
import net.datafaker.Faker;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class FakerPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Locale locale;
    private final Faker faker;

    @Inject
    public FakerPageSourceProvider(FakerConfig config)
    {
        locale = config.getLocale();
        faker = new Faker(locale);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        List<FakerColumnHandle> handles = columns
                .stream()
                .map(FakerColumnHandle.class::cast)
                .collect(toImmutableList());

        FakerSplit fakerSplit = (FakerSplit) split;
        FakerTableHandle fakerTable = (FakerTableHandle) table;
        return new FakerPageSource(locale, fakerTable.seed(), handles, fakerSplit.rowsOffset(), fakerSplit.rowsCount());
    }

    public void validateGenerator(String generator)
    {
        faker.expression(generator);
    }
}
