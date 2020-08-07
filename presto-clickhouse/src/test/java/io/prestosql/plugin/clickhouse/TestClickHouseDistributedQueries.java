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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;

@Test
public class TestClickHouseDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        clickhouseServer.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    // @Override
    // protected boolean supportsArrays()
    // {
    //     // Arrays are supported conditionally. Check the defaults.
    //     return new ClickHouseConfig().getArrayMapping() != ClickHouseConfig.ArrayMapping.DISABLED;
    // }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(clickhouseServer.getJdbcUrl()),
                "tpch.table",
                "(col_required BIGINT," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT," +
                        "col_nonnull_default BIGINT ," +
                        "col_required2 BIGINT) ENGINE='Log'");
    }

    @Override
    public void testCreateTable()
    {
        // currently does not support
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // currently does not support
    }

    @Override
    public void testCommentTable()
    {
        //  currently does not support comment on table
    }

    @Override
    public void ensureDistributedQueryRunner()
    {
        // currently does not support
    }

    @Override
    public void testSetSession()
    {
        // currently does not support
    }

    @Override
    public void testResetSession()
    {
        // currently does not support
    }

    @Override
    public void testRenameTable()
    {
        // currently does not support
    }

    @Override
    public void testCommentColumn()
    {
        // currently does not support
    }

    @Override
    public void testRenameColumn()
    {
        // currently does not support
    }

    @Override
    public void testDropColumn()
    {
        // currently does not support
    }

    @Override
    public void testAddColumn()
    {
        // currently does not support
    }

    @Override
    public void testInsert()
    {
        // currently does not support
    }

    @Override
    public void testInsertWithCoercion()
    {
        // currently does not support
    }

    @Override
    public void testInsertUnicode()
    {
        // currently does not support
    }

    @Override
    public void testInsertArray()
    {
        // currently does not support
    }

    @Override
    public void testDelete()
    {
        // currently does not support
    }

    @Override
    public void testDropTableIfExists()
    {
        // currently does not support
    }

    @Override
    public void testView()
    {
        // currently does not support
    }

    @Override
    public void testViewCaseSensitivity()
    {
        // currently does not support
    }

    @Override
    public void testCompatibleTypeChangeForView()
    {
        // currently does not support
    }

    @Override
    public void testCompatibleTypeChangeForView2()
    {
        // currently does not support
    }

    @Override
    public void testViewMetadata()
    {
        // currently does not support
    }

    @Override
    public void testShowCreateView()
    {
        // currently does not support
    }

    @Override
    public void testQueryLoggingCount()
    {
        // currently does not support
    }

    @Override
    public void testShowSchemasFromOther()
    {
        // currently does not support
    }

    @Override
    public void testSymbolAliasing()
    {
        // currently does not support
    }

    @Override
    public void testNonQueryAccessControl()
    {
        // currently does not support
    }

    @Override
    public void testViewColumnAccessControl()
    {
        // currently does not support
    }

    @Override
    public void testViewFunctionAccessControl()
    {
        // currently does not support
    }

    @Override
    public void testWrittenStats()
    {
        // currently does not support
    }

    @Override
    public void testCreateSchema()
    {
        // currently does not support
    }

    @Override
    public void testInsertForDefaultColumn()
    {
        // currently does not support
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        // currently does not support
    }

    @Override
    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        // currently does not support
    }

    @Override
    public void testAggregationOverUnknown()
    {
        // currently does not support
    }

    @Override
    public void testLimitIntMax()
    {
        // currently does not support
    }

    @Override
    public void testNonDeterministic()
    {
        // currently does not support
    }

    @Override
    public void testComplexQuery()
    {
        // currently does not support
    }

    @Override
    public void testDistinctMultipleFields()
    {
        // currently does not support
    }

    @Override
    public void testArithmeticNegation()
    {
        // currently does not support
    }

    @Override
    public void testDistinct()
    {
        // currently does not support
    }

    @Override
    public void testDistinctHaving()
    {
        // currently does not support
    }

    @Override
    public void testDistinctLimit()
    {
        // currently does not support
    }

    @Override
    public void testDistinctWithOrderBy()
    {
        // currently does not support
    }

    @Override
    public void testRepeatedAggregations()
    {
        // currently does not support
    }

    @Override
    public void testRepeatedOutputs()
    {
        // currently does not support
    }

    @Override
    public void testRepeatedOutputs2()
    {
        // currently does not support
    }

    @Override
    public void testLimit()
    {
        // currently does not support
    }

    @Override
    public void testLimitWithAggregation()
    {
        // currently does not support
    }

    @Override
    public void testLimitInInlineView()
    {
        // currently does not support
    }

    @Override
    public void testCountAll()
    {
        // currently does not support
    }

    @Override
    public void testCountColumn()
    {
        // currently does not support
    }

    @Override
    public void testSelectAllFromTable()
    {
        // currently does not support
    }

    @Override
    public void testSelectAllFromOuterScopeTable()
    {
        // currently does not support
    }

    @Override
    public void testSelectAllFromRow()
    {
        // currently does not support
    }

    @Override
    public void testAverageAll()
    {
        // currently does not support
    }

    @Override
    public void testRollupOverUnion()
    {
        // currently does not support
    }

    @Override
    public void testIntersect()
    {
        // currently does not support
    }

    @Override
    public void testIntersectWithAggregation()
    {
        // currently does not support
    }

    @Override
    public void testExcept()
    {
        // currently does not support
    }

    @Override
    public void testExceptWithAggregation()
    {
        // currently does not support
    }

    @Override
    public void testSelectWithComparison()
    {
        // currently does not support
    }

    @Override
    public void testInlineView()
    {
        // currently does not support
    }

    @Override
    public void testAliasedInInlineView()
    {
        // currently does not support
    }

    @Override
    public void testInlineViewWithProjections()
    {
        // currently does not support
    }

    @Override
    public void testMaxBy()
    {
        // currently does not support
    }

    @Override
    public void testMaxByN()
    {
        // currently does not support
    }

    @Override
    public void testMinBy()
    {
        // currently does not support
    }

    @Override
    public void testMinByN()
    {
        // currently does not support
    }

    @Override
    public void testHaving()
    {
        // currently does not support
    }

    @Override
    public void testHaving2()
    {
        // currently does not support
    }

    @Override
    public void testHaving3()
    {
        // currently does not support
    }

    @Override
    public void testHavingWithoutGroupBy()
    {
        // currently does not support
    }

    @Override
    public void testColumnAliases()
    {
        // currently does not support
    }

    @Override
    public void testCast()
    {
        // currently does not support
    }

    @Override
    public void testQuotedIdentifiers()
    {
        // currently does not support
    }

    @Override
    public void testIn()
    {
        // currently does not support
    }

    @Override
    public void testLargeIn()
    {
        // currently does not support
    }

    @Override
    public void testShowSchemas()
    {
        // currently does not support
    }

    @Override
    public void testShowSchemasFrom()
    {
        // currently does not support
    }

    @Override
    public void testShowSchemasLike()
    {
        // currently does not support
    }

    @Override
    public void testShowSchemasLikeWithEscape()
    {
        // currently does not support
    }

    @Override
    public void testShowTables()
    {
        // currently does not support
    }

    @Override
    public void testShowTablesFrom()
    {
        // currently does not support
    }

    @Override
    public void testShowTablesLike()
    {
        // currently does not support
    }

    @Override
    public void testShowColumns()
    {
        // currently does not support
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        // currently does not support
    }

    @Override
    public void testInformationSchemaUppercaseName()
    {
        // currently does not support
    }

    @Override
    public void testSelectColumnOfNulls()
    {
        // currently does not support
    }

    @Override
    public void testSelectCaseInsensitive()
    {
        // currently does not support
    }

    @Override
    public void testTopN()
    {
        // currently does not support
    }

    @Override
    public void testTopNByMultipleFields()
    {
        // currently does not support
    }

    @Override
    public void testLimitPushDown()
    {
        // currently does not support
    }

    @Override
    public void testScalarSubquery()
    {
        // currently does not support
    }

    @Override
    public void testExistsSubquery()
    {
        // currently does not support
    }

    @Override
    public void testScalarSubqueryWithGroupBy()
    {
        // currently does not support
    }

    @Override
    public void testExistsSubqueryWithGroupBy()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedScalarSubqueries()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedInPredicateSubqueries()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedExistsSubqueries()
    {
        // currently does not support
    }

    @Override
    public void testTwoCorrelatedExistsSubqueries()
    {
        // currently does not support
    }

    @Override
    public void testPredicatePushdown()
    {
        // currently does not support
    }

    @Override
    public void testGroupByKeyPredicatePushdown()
    {
        // currently does not support
    }

    @Override
    public void testNonDeterministicTableScanPredicatePushdown()
    {
        // currently does not support
    }

    @Override
    public void testNonDeterministicAggregationPredicatePushdown()
    {
        // currently does not support
    }

    @Override
    public void testUnionAllPredicateMoveAroundWithOverlappingProjections()
    {
        // currently does not support
    }

    @Override
    public void testTableSampleBernoulliBoundaryValues()
    {
        // currently does not support
    }

    @Override
    public void testTableSampleBernoulli()
    {
        // currently does not support
    }

    @Override
    public void testFilterPushdownWithAggregation()
    {
        // currently does not support
    }

    @Override
    public void testAccessControl()
    {
        // currently does not support
    }

    @Override
    public void testCorrelatedJoin()
    {
        // currently does not support
    }

    @Override
    public void testPruningCountAggregationOverScalar()
    {
        // currently does not support
    }

    @Override
    public void testSubqueriesWithDisjunction()
    {
        // currently does not support
    }
}
