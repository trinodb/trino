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
package io.prestosql.execution;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.TestingMetadata;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;

public abstract class AbstractCoordinatorDynamicFilteringTest
        extends AbstractTestQueryFramework
{
    private static final TestingMetadata.TestingColumnHandle SUPP_KEY_HANDLE = new TestingMetadata.TestingColumnHandle("suppkey", 2, BIGINT);

    private final Map<String, TupleDomain<ColumnHandle>> expectedDynamicFilter = new ConcurrentHashMap<>();
    private final AtomicInteger dynamicFilterCounter = new AtomicInteger();

    @Test(timeOut = 30_000)
    public void testJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testBroadcastJoinWithEmptyBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'",
                TupleDomain.none());
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testBroadcastJoinWithSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                withBroadcastJoin(),
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name = 'Supplier#000000001'",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 1L))));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithNonSelectiveBuildSide()
    {
        assertQueryDynamicFilters(
                "SELECT * FROM lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, true)), false))));
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        assertQueryDynamicFilters(
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "lineitem JOIN tpch.tiny.supplier ON lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN tpch.tiny.partsupp ON t.suppkey = partsupp.suppkey AND partsupp.suppkey IN (2, 3)",
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        SUPP_KEY_HANDLE,
                        singleValue(BIGINT, 2L))));
    }

    protected TupleDomain<ColumnHandle> getExpectedDynamicFilter(ConnectorSession session)
    {
        return expectedDynamicFilter.get(session.getSource().get());
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private void assertQueryDynamicFilters(@Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        assertQueryDynamicFilters(getSession(), query, expectedTupleDomain);
    }

    private void assertQueryDynamicFilters(Session session, @Language("SQL") String query, TupleDomain<ColumnHandle> expectedTupleDomain)
    {
        // Due to https://github.com/cbeust/testng/issues/144 bug
        // tests from abstract classes will be executed in parallel.
        // Therefore expected dynamic filter needs to be passed in thread-safe way.
        String dynamicFilterNumber = String.valueOf(dynamicFilterCounter.getAndIncrement());
        expectedDynamicFilter.put(dynamicFilterNumber, expectedTupleDomain);
        computeActual(
                Session.builder(session)
                        .setSource(dynamicFilterNumber)
                        .build(),
                query);
    }
}
