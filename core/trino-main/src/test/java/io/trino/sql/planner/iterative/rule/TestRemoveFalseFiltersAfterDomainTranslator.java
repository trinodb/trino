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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.testing.TestingTransactionHandle;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.lang.String.format;

public class TestRemoveFalseFiltersAfterDomainTranslator
        extends BaseRuleTest
{
    private static final ConnectorTableHandle MOCK_CONNECTOR_TABLE_HANDLE = new MockConnectorTableHandle(new SchemaTableName("schema", "table"));
    private static final ColumnHandle MOCK_COLUMN_HANDLE = new MockConnectorColumnHandle("col", BIGINT);

    private RemoveFalseFiltersAfterDomainTranslator removeFalseFiltersAfterDomainTranslator;
    private TableHandle tableHandle;

    @BeforeClass
    public void setUpBeforeClass()
    {
        removeFalseFiltersAfterDomainTranslator = new RemoveFalseFiltersAfterDomainTranslator(tester().getPlannerContext());
        tableHandle = new TableHandle(
                tester().getCurrentCatalogHandle(),
                MOCK_CONNECTOR_TABLE_HANDLE,
                TestingTransactionHandle.create());
    }

    @Test
    public void testDoesNotFireIfPredicateIsNotFalse()
    {
        assertDoesNotFire("TRUE");
        assertDoesNotFire("NOT (FALSE)");
        assertDoesNotFire("col = BIGINT '44'");
    }

    @Test
    public void testEliminateTableScanWhenPredicateIsFalse()
    {
        assertFires("FALSE");
        assertFires("NOT (TRUE)");
    }

    @Test
    public void testEliminateTableScanWhenPredicateIsNull()
    {
        assertFiresWithAndWithoutNegation("CAST(null AS boolean)");

        assertFires("col = BIGINT '44' AND CAST(null AS boolean)");

        assertFiresWithAndWithoutNegation("col > CAST(null AS BIGINT)");
        assertFiresWithAndWithoutNegation("col >= CAST(null AS BIGINT)");
        assertFiresWithAndWithoutNegation("col < CAST(null AS BIGINT)");
        assertFiresWithAndWithoutNegation("col <= CAST(null AS BIGINT)");
        assertFiresWithAndWithoutNegation("col = CAST(null AS BIGINT)");
        assertFiresWithAndWithoutNegation("col != CAST(null AS BIGINT)");

        assertFiresWithAndWithoutNegation("col IN (CAST(null AS BIGINT))");

        assertFires("col BETWEEN (CAST(null AS BIGINT)) AND BIGINT '44'");
        assertFires("col BETWEEN BIGINT '44' AND (CAST(null AS BIGINT))");
    }

    private void assertDoesNotFire(@Language("SQL") String sql)
    {
        tester().assertThat(removeFalseFiltersAfterDomainTranslator)
                .on(p -> p.filter(expression(sql),
                        p.tableScan(
                                tableHandle,
                                ImmutableList.of(p.symbol("col", BIGINT)),
                                ImmutableMap.of(p.symbol("col", BIGINT), MOCK_COLUMN_HANDLE))))
                .doesNotFire();
    }

    private void assertFiresWithAndWithoutNegation(@Language("SQL") String sql)
    {
        assertFires(sql);
        assertFires(format("NOT (%s)", sql));
    }

    private void assertFires(@Language("SQL") String sql)
    {
        tester().assertThat(removeFalseFiltersAfterDomainTranslator)
                .on(p -> p.filter(expression(sql),
                        p.tableScan(
                                tableHandle,
                                ImmutableList.of(p.symbol("col", BIGINT)),
                                ImmutableMap.of(p.symbol("col", BIGINT), MOCK_COLUMN_HANDLE))))
                .matches(values(ImmutableList.of("A"), ImmutableList.of()));
    }
}
