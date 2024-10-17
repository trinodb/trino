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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import jakarta.inject.Inject;
import net.datafaker.Faker;

import java.util.List;
import java.util.Random;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class FakerPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Random random;
    private final Faker faker;

    @Inject
    public FakerPageSourceProvider()
    {
        random = new Random();
        faker = new Faker(random);
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
        List<FakerColumnHandle> handles = columns
                .stream()
                .map(FakerColumnHandle.class::cast)
                .collect(toImmutableList());

        FakerTableHandle fakerTable = (FakerTableHandle) table;
        FakerSplit fakerSplit = (FakerSplit) split;
        return new FakerPageSource(faker, random, handles, fakerTable.constraint(), fakerSplit.limit());
    }

    public void validateGenerator(String generator)
    {
        faker.expression(generator);
    }
}
