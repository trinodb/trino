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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.testing.PlanTester;
import io.trino.testing.tpch.IndexedTpchConnectorFactory;
import io.trino.testing.tpch.TpchIndexSpec;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIndexJoinPlans
        extends BasePlanTest
{
    private static final String CATALOG = "tpch_indexed";

    private static final TpchIndexSpec INDEX_SPEC = new TpchIndexSpec.Builder()
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey"))
            .build();

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        PlanTester planTester = PlanTester.create(session);
        planTester.createCatalog(CATALOG, new IndexedTpchConnectorFactory(INDEX_SPEC, 1), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testIndexJoinOverDeterministicFilter()
    {
        assertThat(hasIndexJoin(
                """
                SELECT *
                FROM (SELECT orderkey FROM lineitem WHERE partkey % 8 = 0) l
                JOIN (SELECT * FROM orders WHERE custkey % 2 = 0) o
                  ON l.orderkey = o.orderkey
                """))
                .isTrue();
    }

    @Test
    public void testIndexJoinOverDeterministicProjection()
    {
        assertThat(hasIndexJoin(
                """
                SELECT *
                FROM (SELECT orderkey FROM lineitem WHERE partkey % 8 = 0) l
                JOIN (SELECT orderkey, custkey % 2 AS parity FROM orders) o
                  ON l.orderkey = o.orderkey
                """))
                .isTrue();
    }

    @Test
    public void testNoIndexJoinOverNondeterministicFilter()
    {
        assertThat(hasIndexJoin(
                """
                SELECT *
                FROM (SELECT orderkey FROM lineitem WHERE partkey % 8 = 0) l
                JOIN (SELECT * FROM orders WHERE random() < 0.5) o
                  ON l.orderkey = o.orderkey
                """))
                .isFalse();
    }

    @Test
    public void testNoIndexJoinOverNondeterministicProjection()
    {
        assertThat(hasIndexJoin(
                """
                SELECT *
                FROM (SELECT orderkey FROM lineitem WHERE partkey % 8 = 0) l
                JOIN (SELECT orderkey, random() AS r FROM orders) o
                  ON l.orderkey = o.orderkey
                """))
                .isFalse();
    }

    private boolean hasIndexJoin(@Language("SQL") String sql)
    {
        return searchFrom(plan(sql).getRoot())
                .whereIsInstanceOfAny(IndexJoinNode.class)
                .matches();
    }
}
