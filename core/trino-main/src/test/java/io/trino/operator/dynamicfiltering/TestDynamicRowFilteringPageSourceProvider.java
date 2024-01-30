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
package io.trino.operator.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicRowFilteringPageSourceProvider
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final Session SESSION = TestingSession.testSession();
    private static final ColumnHandle COLUMN1 = new TestingColumnHandle("COLUMN1");
    private static final ColumnHandle COLUMN2 = new TestingColumnHandle("COLUMN2");
    private static final ColumnHandle COLUMN3 = new TestingColumnHandle("COLUMN3");

    @Test
    public void testGetUnenforcedPredicate()
    {
        DynamicRowFilteringPageSourceProvider provider = new DynamicRowFilteringPageSourceProvider(new DynamicPageFilterCache(TYPE_OPERATORS));
        assertThat(provider.getUnenforcedPredicate(
                new TestingConnectorPageSourceProvider(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                COLUMN1, singleValue(BIGINT, 10L),
                                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 22L, 23L)))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L))))),
                SESSION,
                SESSION.toConnectorSession(),
                createRemoteSplit(),
                new TestingTableHandle(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L, 23L)),
                        COLUMN3, singleValue(BIGINT, 30L)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN1, singleValue(BIGINT, 10L),
                        COLUMN2, singleValue(BIGINT, 20L))));

        // delegate provider returns TupleDomain.none()
        assertThat(provider.getUnenforcedPredicate(
                new TestingConnectorPageSourceProvider(TupleDomain.none(), TupleDomain.none()),
                SESSION,
                SESSION.toConnectorSession(),
                createRemoteSplit(),
                new TestingTableHandle(),
                TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.none());

        // delegate provider returns TupleDomain.all()
        assertThat(provider.getUnenforcedPredicate(
                new TestingConnectorPageSourceProvider(TupleDomain.all(), TupleDomain.all()),
                SESSION,
                SESSION.toConnectorSession(),
                createRemoteSplit(),
                new TestingTableHandle(),
                TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.all());
    }

    private static class TestingConnectorPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        private final TupleDomain<ColumnHandle> unenforcedPredicate;
        private final TupleDomain<ColumnHandle> prunedPredicate;

        public TestingConnectorPageSourceProvider(TupleDomain<ColumnHandle> unenforcedPredicate, TupleDomain<ColumnHandle> prunedPredicate)
        {
            this.unenforcedPredicate = unenforcedPredicate;
            this.prunedPredicate = prunedPredicate;
        }

        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TupleDomain<ColumnHandle> getUnenforcedPredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> dynamicFilter)
        {
            return unenforcedPredicate;
        }

        @Override
        public TupleDomain<ColumnHandle> prunePredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> predicate)
        {
            return prunedPredicate;
        }
    }
}
