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
package io.prestosql.sql.analyzer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.informationschema.InformationSchemaConnector;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.memory.NodeMemoryConfig;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.assertions.PrestoExceptionAssert;
import io.prestosql.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.prestosql.connector.CatalogName.createInformationSchemaCatalogName;
import static io.prestosql.connector.CatalogName.createSystemTablesCatalogName;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.prestosql.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_NAMED_QUERY;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_AGGREGATE;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.INVALID_LITERAL;
import static io.prestosql.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.prestosql.spi.StandardErrorCode.INVALID_VIEW;
import static io.prestosql.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.prestosql.spi.StandardErrorCode.MISMATCHED_COLUMN_ALIASES;
import static io.prestosql.spi.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.prestosql.spi.StandardErrorCode.MISSING_COLUMN_NAME;
import static io.prestosql.spi.StandardErrorCode.MISSING_GROUP_BY;
import static io.prestosql.spi.StandardErrorCode.MISSING_ORDER_BY;
import static io.prestosql.spi.StandardErrorCode.MISSING_OVER;
import static io.prestosql.spi.StandardErrorCode.MISSING_SCHEMA_NAME;
import static io.prestosql.spi.StandardErrorCode.NESTED_AGGREGATION;
import static io.prestosql.spi.StandardErrorCode.NESTED_WINDOW;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.NULL_TREATMENT_NOT_ALLOWED;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_GROUPING_SETS;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.StandardErrorCode.VIEW_IS_RECURSIVE;
import static io.prestosql.spi.StandardErrorCode.VIEW_IS_STALE;
import static io.prestosql.spi.connector.ConnectorViewDefinition.ViewColumn;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;

@Test(singleThreaded = true)
public class TestAnalyzer
{
    private static final String TPCH_CATALOG = "tpch";
    private static final CatalogName TPCH_CATALOG_NAME = new CatalogName(TPCH_CATALOG);
    private static final String SECOND_CATALOG = "c2";
    private static final CatalogName SECOND_CATALOG_NAME = new CatalogName(SECOND_CATALOG);
    private static final String THIRD_CATALOG = "c3";
    private static final CatalogName THIRD_CATALOG_NAME = new CatalogName(THIRD_CATALOG);
    private static final String CATALOG_FOR_IDENTIFIER_CHAIN_TESTS = "cat";
    private static final CatalogName CATALOG_FOR_IDENTIFIER_CHAIN_TESTS_NAME = new CatalogName(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS);
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();
    private static final Session CLIENT_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .build();
    private static final Session CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS = testSessionBuilder()
            .setCatalog(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS)
            .setSchema("a")
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private Metadata metadata;

    @Test
    public void testTooManyArguments()
    {
        assertFails("SELECT greatest(" + Joiner.on(", ").join(nCopies(128, "rand()")) + ")")
                .hasErrorCode(TOO_MANY_ARGUMENTS);
    }

    @Test
    public void testNonComparableGroupBy()
    {
        assertFails("SELECT * FROM (SELECT approx_set(1)) GROUP BY 1")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNonComparableWindowPartition()
    {
        assertFails("SELECT row_number() OVER (PARTITION BY t.x) FROM (VALUES(CAST (NULL AS HyperLogLog))) AS t(x)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNonComparableWindowOrder()
    {
        assertFails("SELECT row_number() OVER (ORDER BY t.x) FROM (VALUES(color('red'))) AS t(x)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNonComparableDistinctAggregation()
    {
        assertFails("SELECT count(DISTINCT x) FROM (SELECT approx_set(1) x)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNonComparableDistinct()
    {
        assertFails("SELECT DISTINCT * FROM (SELECT approx_set(1) x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT DISTINCT x FROM (SELECT approx_set(1) x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT DISTINCT ROW(1, approx_set(1)).* from t1")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testInSubqueryTypes()
    {
        assertFails("SELECT * FROM (VALUES 'a') t(y) WHERE y IN (VALUES 1)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT (VALUES true) IN (VALUES 1)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testScalarSubQuery()
    {
        analyze("SELECT 'a', (VALUES 1) GROUP BY 1");
        analyze("SELECT 'a', (SELECT (1))");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) = 2");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (VALUES 1)");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (2)");
        analyze("SELECT * FROM (SELECT 1) t1(x) WHERE x IN (SELECT 1)");
    }

    @Test
    public void testRowDereferenceInCorrelatedSubquery()
    {
        assertFails("WITH " +
                "    t(b) AS (VALUES row(cast(row(1) AS row(a bigint))))," +
                "    u(b) AS (VALUES row(cast(row(1, 1) AS row(a bigint, b bigint))))" +
                "SELECT b " +
                "FROM t " +
                "WHERE EXISTS (" +
                "    SELECT b.a" + // this should be considered group-variant since it references u.b.a
                "    FROM u" +
                "    GROUP BY b.b" +
                ")")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:171: 'b.a' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testReferenceToOutputColumnFromOrderByAggregation()
    {
        assertFails("SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 1:71: Invalid reference to output projection attribute from ORDER BY aggregation");

        assertFails("SELECT DISTINCT a AS a, max(a) AS c from (VALUES (1, 2)) t(a, b) GROUP BY a ORDER BY max(a)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 1:90: Invalid reference to output projection attribute from ORDER BY aggregation");

        assertFails("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY MAX(a.someField)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 1:102: Invalid reference to output projection attribute from ORDER BY aggregation");

        assertFails("SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, z -> x))")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 1:81: Invalid reference to output projection attribute from ORDER BY aggregation");

        assertFails("SELECT row_number() over() as a from (values (41, 42), (-41, -42)) t(a,b) group by a+b order by a+b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("\\Qline 1:98: '(a + b)' must be an aggregate expression or appear in GROUP BY clause\\E");
    }

    @Test
    public void testHavingReferencesOutputAlias()
    {
        assertFails("SELECT sum(a) x FROM t1 HAVING x > 5")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testSelectAllColumns()
    {
        // wildcard without FROM
        assertFails("SELECT *")
                .hasErrorCode(COLUMN_NOT_FOUND);

        // wildcard with invalid prefix
        assertFails("SELECT foo.* FROM t1")
                .hasErrorCode(TABLE_NOT_FOUND);

        assertFails("SELECT a.b.c.d.* FROM t1")
                .hasErrorCode(TABLE_NOT_FOUND);

        // aliases mismatch
        assertFails("SELECT (1, 2).* AS (a) FROM t1")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES);

        // wildcard with no RowType expression
        assertFails("SELECT non_row.* FROM (VALUES ('true', 1)) t(non_row, b)")
                .hasErrorCode(TABLE_NOT_FOUND);

        // wildcard with no RowType expression nested in a row
        assertFails("SELECT t.row.non_row.* FROM (VALUES (CAST(ROW('true') AS ROW(non_row boolean)), 1)) t(row, b)")
                .hasErrorCode(TYPE_MISMATCH);

        // reference to outer scope relation
        assertFails("SELECT (SELECT outer_relation.* FROM (VALUES 1) inner_relation) FROM (values 2) outer_relation")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testGroupByWithWildcard()
    {
        assertFails("SELECT * FROM t1 GROUP BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 ON u1.a = u2.a GROUP BY u1.a, u2.a, 3")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
    }

    @Test
    public void testAsteriskedIdentifierChainResolution()
    {
        // identifier chain of length 2; match to table and field in immediate scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT a.b.* FROM a.b, t1 AS a")
                .hasErrorCode(AMBIGUOUS_NAME);

        // identifier chain of length 2; match to table and field in outer scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM (VALUES 1) v) FROM a.b, t1 AS a")
                .hasErrorCode(AMBIGUOUS_NAME);

        // identifier chain of length 3; match to table and field in immediate scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT cat.a.b.* FROM cat.a.b, t2 AS cat")
                .hasErrorCode(AMBIGUOUS_NAME);

        // identifier chain of length 3; match to table and field in outer scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT cat.a.b.* FROM (VALUES 1) v) FROM cat.a.b, t2 AS cat")
                .hasErrorCode(AMBIGUOUS_NAME);

        // identifier chain of length 2; no ambiguity: table match in closer scope than field match
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM a.b) FROM t1 AS a");

        // identifier chain of length 2; no ambiguity: field match in closer scope than table match
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM t5 AS a) FROM a.b");

        // identifier chain of length 2; no ambiguity: only field match in outer scope
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM (VALUES 1) v) FROM t5 AS a");

        // identifier chain of length 2; no ambiguity: only table match in outer scope (not supported)
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM (VALUES 1) v) FROM a.b")
                .hasErrorCode(NOT_SUPPORTED);

        // identifier chain of length 1; only table match allowed, no potential ambiguity detection (could match field b from t1)
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT b.* FROM b, t1");

        // identifier chain of length 1; only table match allowed, referencing field not qualified by table alias not allowed
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT b.* FROM t1")
                .hasErrorCode(TABLE_NOT_FOUND);

        // identifier chain of length 3; illegal reference: multi-identifier table reference + field reference
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT a.t1.b.* FROM a.t1")
                .hasErrorCode(TABLE_NOT_FOUND);
        // the above query fixed by the use of table alias
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT alias.b.* FROM a.t1 as alias");

        // identifier chain of length 4; illegal reference: multi-identifier table reference + field reference
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT cat.a.t1.b.* FROM cat.a.t1")
                .hasErrorCode(TABLE_NOT_FOUND);
        // the above query fixed by the use of table alias
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT alias.b.* FROM cat.a.t1 AS alias");

        // reference to nested row qualified by single-identifier table alias
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT t3.b.f1.* FROM t3");

        // reference to double-nested row qualified by single-identifier table alias
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT t4.b.f1.f11.* FROM t4");

        // table reference by the suffix of table's qualified name
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT b.* FROM cat.a.b");
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT a.b.* FROM cat.a.b");
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT b.* FROM a.b");

        // ambiguous field references in immediate scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT a.b.* FROM t4 AS a, t5 AS a")
                .hasErrorCode(AMBIGUOUS_NAME);

        // ambiguous field references in outer scope
        assertFails(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM (VALUES 1) v) FROM t4 AS a, t5 AS a")
                .hasErrorCode(AMBIGUOUS_NAME);
    }

    @Test
    public void testGroupByInvalidOrdinal()
    {
        assertFails("SELECT * FROM t1 GROUP BY 10")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        assertFails("SELECT * FROM t1 GROUP BY 0")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
    }

    @Test
    public void testGroupByWithSubquerySelectExpression()
    {
        analyze("SELECT (SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT (SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT (SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails("SELECT (SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:16: Subquery uses 'u.a' which must appear in GROUP BY clause");

        assertFails("SELECT (SELECT a+2) FROM t1 GROUP BY a+1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:16: Subquery uses 'a' which must appear in GROUP BY clause");

        assertFails("SELECT (SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:36: Subquery uses 'u.a' which must appear in GROUP BY clause");

        // (t1.)a is not part of GROUP BY
        assertFails("SELECT (SELECT a as a) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT (SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithExistsSelectExpression()
    {
        analyze("SELECT EXISTS(SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT EXISTS(SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT EXISTS(SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails("SELECT EXISTS(SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:22: Subquery uses 'u.a' which must appear in GROUP BY clause");

        assertFails("SELECT EXISTS(SELECT a+2) FROM t1 GROUP BY a+1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:22: Subquery uses 'a' which must appear in GROUP BY clause");

        assertFails("SELECT EXISTS(SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:42: Subquery uses 'u.a' which must appear in GROUP BY clause");

        // (t1.)a is not part of GROUP BY
        assertFails("SELECT EXISTS(SELECT a as a) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT EXISTS(SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithSubquerySelectExpressionWithDereferenceExpression()
    {
        analyze("SELECT (SELECT t.a.someField) " +
                "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                "GROUP BY a");

        assertFails("SELECT (SELECT t.a.someField) " +
                "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                "GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:16: Subquery uses 't.a' which must appear in GROUP BY clause");
    }

    @Test
    public void testOrderByInvalidOrdinal()
    {
        assertFails("SELECT * FROM t1 ORDER BY 10")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        assertFails("SELECT * FROM t1 ORDER BY 0")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
    }

    @Test
    public void testOrderByNonComparable()
    {
        assertFails("SELECT x FROM (SELECT approx_set(1) x) ORDER BY 1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM (SELECT approx_set(1) x) ORDER BY 1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT x FROM (SELECT approx_set(1) x) ORDER BY x")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testOffsetInvalidRowCount()
    {
        assertFails("SELECT * FROM t1 OFFSET 987654321098765432109876543210 ROWS")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testFetchFirstInvalidRowCount()
    {
        assertFails("SELECT * FROM t1 FETCH FIRST 987654321098765432109876543210 ROWS ONLY")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM t1 FETCH FIRST 0 ROWS ONLY")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testFetchFirstWithTiesMissingOrderBy()
    {
        assertFails("SELECT * FROM t1 FETCH FIRST 5 ROWS WITH TIES")
                .hasErrorCode(MISSING_ORDER_BY);

        // ORDER BY clause must be in the same scope as FETCH FIRST WITH TIES
        assertFails("SELECT * FROM (SELECT * FROM (values 1, 3, 2) t(a) ORDER BY a) FETCH FIRST 5 ROWS WITH TIES")
                .hasErrorCode(MISSING_ORDER_BY);
    }

    @Test
    public void testLimitInvalidRowCount()
    {
        assertFails("SELECT * FROM t1 LIMIT 987654321098765432109876543210")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNestedAggregation()
    {
        assertFails("SELECT sum(count(*)) FROM t1")
                .hasErrorCode(NESTED_AGGREGATION);
    }

    @Test
    public void testAggregationsNotAllowed()
    {
        assertFails("SELECT * FROM t1 WHERE sum(a) > 1")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 GROUP BY sum(a)")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
    }

    @Test
    public void testWindowsNotAllowed()
    {
        assertFails("SELECT * FROM t1 WHERE foo() over () > 1")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 GROUP BY rank() over ()")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT 1 FROM (VALUES 1) HAVING count(*) OVER () > 1")
                .hasErrorCode(NESTED_WINDOW);
    }

    @Test
    public void testGrouping()
    {
        analyze("SELECT a, b, sum(c), grouping(a, b) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))");
        analyze("SELECT grouping(t1.a) FROM t1 GROUP BY a");
        analyze("SELECT grouping(b) FROM t1 GROUP BY t1.b");
        analyze("SELECT grouping(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupingNotAllowed()
    {
        assertFails("SELECT a, b, sum(c) FROM t1 WHERE grouping(a, b) GROUP BY GROUPING SETS ((a), (a, b))")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT a, b, sum(c) FROM t1 GROUP BY grouping(a, b)")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT t1.a, t1.b FROM t1 JOIN t2 ON grouping(t1.a, t1.b) > t2.a")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);

        assertFails("SELECT grouping(a) FROM t1")
                .hasErrorCode(MISSING_GROUP_BY);
        assertFails("SELECT * FROM t1 ORDER BY grouping(a)")
                .hasErrorCode(MISSING_GROUP_BY);
        assertFails("SELECT grouping(a) FROM t1 GROUP BY b")
                .hasErrorCode(INVALID_ARGUMENTS);
        assertFails("SELECT grouping(a.field) FROM (VALUES ROW(CAST(ROW(1) AS ROW(field BIGINT)))) t(a) GROUP BY a.field")
                .hasErrorCode(INVALID_ARGUMENTS);

        assertFails("SELECT a FROM t1 GROUP BY a ORDER BY grouping(a)")
                .hasErrorCode(INVALID_ARGUMENTS);
    }

    @Test
    public void testGroupingTooManyArguments()
    {
        String grouping = "GROUPING(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a)";
        assertFails(format("SELECT a, b, %s + 1 FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping))
                .hasErrorCode(TOO_MANY_ARGUMENTS);
        assertFails(format("SELECT a, b, %s as g FROM t1 GROUP BY a, b HAVING g > 0", grouping))
                .hasErrorCode(TOO_MANY_ARGUMENTS);
        assertFails(format("SELECT a, b, rank() OVER (PARTITION BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping))
                .hasErrorCode(TOO_MANY_ARGUMENTS);
        assertFails(format("SELECT a, b, rank() OVER (PARTITION BY a ORDER BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping))
                .hasErrorCode(TOO_MANY_ARGUMENTS);
    }

    @Test
    public void testInvalidTable()
    {
        assertFails("SELECT * FROM foo.bar.t")
                .hasErrorCode(CATALOG_NOT_FOUND);
        assertFails("SELECT * FROM foo.t")
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertFails("SELECT * FROM foo")
                .hasErrorCode(TABLE_NOT_FOUND);
    }

    @Test
    public void testInvalidSchema()
    {
        assertFails("SHOW TABLES FROM NONEXISTENT_SCHEMA")
                .hasErrorCode(SCHEMA_NOT_FOUND);
        assertFails("SHOW TABLES IN NONEXISTENT_SCHEMA LIKE '%'")
                .hasErrorCode(SCHEMA_NOT_FOUND);
    }

    @Test
    public void testNonAggregate()
    {
        assertFails("SELECT 'a', array[b][1] FROM t1 GROUP BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT a, sum(b) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT sum(b) / a FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT sum(b) / a FROM t1 GROUP BY c")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT sum(b) FROM t1 ORDER BY a + 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT count(*) over (PARTITION BY a) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT count(*) over (ORDER BY a) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT count(*) over (ORDER BY count(*) ROWS a PRECEDING) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN b PRECEDING AND a PRECEDING) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN a PRECEDING AND UNBOUNDED PRECEDING) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
    }

    @Test
    public void testInvalidAttribute()
    {
        assertFails("SELECT f FROM t1")
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails("SELECT * FROM t1 ORDER BY f")
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails("SELECT count(*) FROM t1 GROUP BY f")
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails("SELECT * FROM t1 WHERE f > 1")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testInvalidAttributeCorrectErrorMessage()
    {
        assertFails("SELECT t.y FROM (VALUES 1) t(x)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("\\Qline 1:8: Column 't.y' cannot be resolved\\E");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
    {
        assertFails("SELECT DISTINCT a FROM t1 ORDER BY b")
                .hasErrorCode(EXPRESSION_NOT_IN_DISTINCT);
    }

    @Test
    public void testNonDeterministicOrderBy()
    {
        analyze("SELECT DISTINCT random() as b FROM t1 ORDER BY b");
        analyze("SELECT random() FROM t1 ORDER BY random()");
        analyze("SELECT a FROM t1 ORDER BY random()");
        assertFails("SELECT DISTINCT random() FROM t1 ORDER BY random()")
                .hasErrorCode(EXPRESSION_NOT_IN_DISTINCT);
    }

    @Test
    public void testNonBooleanWhereClause()
    {
        assertFails("SELECT * FROM t1 WHERE a")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), SUM(a) FROM t1");
    }

    @Test
    public void testMultipleDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
    {
        // TODO: analyze output
        analyze("SELECT a x FROM t1 ORDER BY x + 1");
        analyze("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1e0)");
        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY a.someField");
        analyze("SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, x -> x))");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");

        assertFails("SELECT x.c as x\n" +
                "FROM (VALUES 1) x(c)\n" +
                "ORDER BY x.c")
                .hasErrorCode(TYPE_MISMATCH)
                .hasLocation(3, 10);
    }

    @Test
    public void testOrderByWithWildcard()
    {
        // TODO: validate output
        analyze("SELECT t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testOrderByWithGroupByAndSubquerySelectExpression()
    {
        analyze("SELECT a FROM t1 GROUP BY a ORDER BY (SELECT a)");

        assertFails("SELECT a FROM t1 GROUP BY a ORDER BY (SELECT b)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:46: Subquery uses 'b' which must appear in GROUP BY clause");

        analyze("SELECT a AS b FROM t1 GROUP BY t1.a ORDER BY (SELECT b)");

        assertFails("SELECT a AS b FROM t1 GROUP BY t1.a \n" +
                "ORDER BY MAX((SELECT b))")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 2:22: Invalid reference to output projection attribute from ORDER BY aggregation");

        analyze("SELECT a FROM t1 GROUP BY a ORDER BY MAX((SELECT x FROM (VALUES 4) t(x)))");

        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                "FROM (VALUES (1, 2)) t(a, b)\n" +
                "GROUP BY b\n" +
                "ORDER BY (SELECT x.someField)");

        assertFails("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                "FROM (VALUES (1, 2)) t(a, b)\n" +
                "GROUP BY b\n" +
                "ORDER BY MAX((SELECT x.someField))")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 4:22: Invalid reference to output projection attribute from ORDER BY aggregation");
    }

    @Test
    public void testTooManyGroupingElements()
    {
        Session session = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig().setMaxGroupingSets(2048),
                new NodeMemoryConfig()))).build();
        analyze(session, "SELECT a, b, c, d, e, f, g, h, i, j, k, SUM(l)" +
                "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))\n" +
                "t (a, b, c, d, e, f, g, h, i, j, k, l)\n" +
                "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k)");
        assertFails(session, "SELECT a, b, c, d, e, f, g, h, i, j, k, l, SUM(m)" +
                "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))\n" +
                "t (a, b, c, d, e, f, g, h, i, j, k, l, m)\n" +
                "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k, l)")
                .hasErrorCode(TOO_MANY_GROUPING_SETS)
                .hasMessageMatching("line 3:10: GROUP BY has 4096 grouping sets but can contain at most 2048");
        assertFails(session, "SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, SUM(af)" +
                "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, " +
                "17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32))\n" +
                "t (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, af)\n" +
                "GROUP BY CUBE (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae)")
                .hasErrorCode(TOO_MANY_GROUPING_SETS)
                .hasMessageMatching(format("line 3:10: GROUP BY has more than %s grouping sets but can contain at most 2048", Integer.MAX_VALUE));
    }

    @Test
    public void testMismatchedColumnAliasCount()
    {
        assertFails("SELECT * FROM t1 u (x, y)")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES);
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnNonBooleanExpression()
    {
        assertFails("SELECT * FROM t1 JOIN t2 ON 5")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testJoinOnAmbiguousName()
    {
        assertFails("SELECT * FROM t1 JOIN t2 ON a = a")
                .hasErrorCode(AMBIGUOUS_NAME);
    }

    @Test
    public void testNonEquiOuterJoin()
    {
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
    {
        assertFails("SELECT sum(a) FROM t1 HAVING sum(a)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
    {
        assertFails("SELECT a x, b x FROM t1 ORDER BY x")
                .hasErrorCode(AMBIGUOUS_NAME);
        assertFails("SELECT a x, a x FROM t1 ORDER BY x")
                .hasErrorCode(AMBIGUOUS_NAME);
        assertFails("SELECT a, a FROM t1 ORDER BY a")
                .hasErrorCode(AMBIGUOUS_NAME);
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyze("SELECT * FROM t1, t2");
    }

    @Test
    public void testNaturalJoinNotSupported()
    {
        assertFails("SELECT * FROM t1 NATURAL JOIN t2")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testNestedWindowFunctions()
    {
        assertFails("SELECT avg(sum(a) OVER ()) FROM t1")
                .hasErrorCode(NESTED_WINDOW);
        assertFails("SELECT sum(sum(a) OVER ()) OVER () FROM t1")
                .hasErrorCode(NESTED_WINDOW);
        assertFails("SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1")
                .hasErrorCode(NESTED_WINDOW);
        assertFails("SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1")
                .hasErrorCode(NESTED_WINDOW);
    }

    @Test
    public void testWindowAttributesForLagLeadFunctions()
    {
        assertFails("SELECT lag(x, 2) OVER() FROM (VALUES 1, 2, 3, 4, 5) t(x) ")
                .hasErrorCode(MISSING_ORDER_BY);
        assertFails("SELECT lag(x, 2) OVER(ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 1, 2, 3, 4, 5) t(x) ")
                .hasErrorCode(INVALID_WINDOW_FRAME);
    }

    @Test
    public void testWindowFunctionWithoutOverClause()
    {
        assertFails("SELECT row_number()")
                .hasErrorCode(MISSING_OVER);
        assertFails("SELECT coalesce(lead(a), 0) from (values(0)) t(a)")
                .hasErrorCode(MISSING_OVER);
    }

    @Test
    public void testInvalidWindowFrame()
    {
        assertFails("SELECT rank() OVER (ROWS UNBOUNDED FOLLOWING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS 2 FOLLOWING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5 PRECEDING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND 5 PRECEDING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND CURRENT ROW)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (RANGE 2 PRECEDING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (RANGE BETWEEN 2 PRECEDING AND 5 FOLLOWING)")
                .hasErrorCode(INVALID_WINDOW_FRAME);

        assertFails("SELECT rank() OVER (ROWS 5e-1 PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS 'foo' PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
    {
        assertFails("SELECT a, count(DISTINCT b) OVER () FROM t1")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
    {
        // TODO: verify output
        analyze("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");

        assertFails("SELECT a FROM t1 HAVING a = 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:8: 'a' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
    {
        // TODO: verify output
        analyze("WITH AB AS (SELECT * FROM t1) SELECT * FROM ab");
    }

    @Test
    public void testStartTransaction()
    {
        analyze("START TRANSACTION");
        analyze("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        analyze("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        analyze("START TRANSACTION READ ONLY");
        analyze("START TRANSACTION READ WRITE");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY");
        analyze("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    }

    @Test
    public void testCommit()
    {
        analyze("COMMIT");
        analyze("COMMIT WORK");
    }

    @Test
    public void testRollback()
    {
        analyze("ROLLBACK");
        analyze("ROLLBACK WORK");
    }

    @Test
    public void testExplainAnalyze()
    {
        analyze("EXPLAIN ANALYZE SELECT * FROM t1");
    }

    @Test
    public void testInsert()
    {
        assertFails("INSERT INTO t6 (a) SELECT b from t6")
                .hasErrorCode(TYPE_MISMATCH);
        analyze("INSERT INTO t1 SELECT * FROM t1");
        analyze("INSERT INTO t3 SELECT * FROM t3");
        analyze("INSERT INTO t3 SELECT a, b FROM t3");
        assertFails("INSERT INTO t1 VALUES (1, 2)")
                .hasErrorCode(TYPE_MISMATCH);
        analyze("INSERT INTO t5 (a) VALUES(null)");

        // ignore t5 hidden column
        analyze("INSERT INTO t5 VALUES (1)");

        // fail if hidden column provided
        assertFails("INSERT INTO t5 VALUES (1, 2)")
                .hasErrorCode(TYPE_MISMATCH);

        // note b is VARCHAR, while a,c,d are BIGINT
        analyze("INSERT INTO t6 (a) SELECT a from t6");
        analyze("INSERT INTO t6 (a) SELECT c from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT * from t6");
        analyze("INSERT INTO t6 (A,B,C,D) SELECT * from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT d,b,c,a from t6");
        assertFails("INSERT INTO t6 (a) SELECT b from t6")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t6 (unknown) SELECT * FROM t6")
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails("INSERT INTO t6 (a, a) SELECT * FROM t6")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);
        assertFails("INSERT INTO t6 (a, A) SELECT * FROM t6")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);

        // b is bigint, while a is double, coercion from b to a is possible
        analyze("INSERT INTO t7 (b) SELECT (a) FROM t7 ");
        assertFails("INSERT INTO t7 (a) SELECT (b) FROM t7")
                .hasErrorCode(TYPE_MISMATCH);

        // d is array of bigints, while c is array of doubles, coercion from d to c is possible
        analyze("INSERT INTO t7 (d) SELECT (c) FROM t7 ");
        assertFails("INSERT INTO t7 (c) SELECT (d) FROM t7 ")
                .hasErrorCode(TYPE_MISMATCH);

        analyze("INSERT INTO t7 (d) VALUES (ARRAY[null])");

        analyze("INSERT INTO t6 (d) VALUES (1), (2), (3)");
        analyze("INSERT INTO t6 (a,b,c,d) VALUES (1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'c', 3, 3), (4, 'd', 4, 4)");
    }

    @Test
    public void testInvalidInsert()
    {
        assertFails("INSERT INTO foo VALUES (1)")
                .hasErrorCode(TABLE_NOT_FOUND);
        assertFails("INSERT INTO v1 VALUES (1)")
                .hasErrorCode(NOT_SUPPORTED);

        // fail if inconsistent fields count
        assertFails("INSERT INTO t1 (a) VALUES (1), (1, 2)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t1 (a, b) VALUES (1), (1, 2)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t1 (a, b) VALUES (1, 2), (1, 2), (1, 2, 3)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b', 'c')")
                .hasErrorCode(TYPE_MISMATCH);

        // fail if mismatched column types
        assertFails("INSERT INTO t1 (a, b) VALUES ('a', 'b'), (1, 'b')")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b'), (1, 'b')")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testDuplicateWithQuery()
    {
        assertFails("WITH a AS (SELECT * FROM t1)," +
                "     a AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(DUPLICATE_NAMED_QUERY);
    }

    @Test
    public void testCaseInsensitiveDuplicateWithQuery()
    {
        assertFails("WITH a AS (SELECT * FROM t1)," +
                "     A AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(DUPLICATE_NAMED_QUERY);
    }

    @Test
    public void testWithForwardReference()
    {
        assertFails("WITH a AS (SELECT * FROM b)," +
                "     b AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(TABLE_NOT_FOUND);
    }

    @Test
    public void testExpressions()
    {
        // logical not
        assertFails("SELECT NOT 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // logical and/or
        assertFails("SELECT 1 AND TRUE FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT TRUE AND 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 OR TRUE FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT TRUE OR 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // comparison
        assertFails("SELECT 1 = 'a' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // nullif
        assertFails("SELECT NULLIF(1, 'a') FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // case
        assertFails("SELECT CASE WHEN TRUE THEN 'a' ELSE 1 END FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CASE WHEN '1' THEN 1 ELSE 2 END FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        assertFails("SELECT CASE 1 WHEN 'a' THEN 2 END FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CASE 1 WHEN 1 THEN 2 ELSE 'a' END FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // coalesce
        assertFails("SELECT COALESCE(1, 'a') FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // cast
        assertFails("SELECT CAST(date '2014-01-01' AS bigint)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT TRY_CAST(date '2014-01-01' AS bigint)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CAST(null AS UNKNOWN)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CAST(1 AS MAP)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CAST(1 AS ARRAY)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT CAST(1 AS ROW)")
                .hasErrorCode(TYPE_MISMATCH);

        // arithmetic unary
        assertFails("SELECT -'a' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT +'a' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // arithmetic addition/subtraction
        assertFails("SELECT 'a' + 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 + 'a'  FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 'a' - 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 - 'a' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // like
        assertFails("SELECT 1 LIKE 'a' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 'a' LIKE 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 'a' LIKE 'b' ESCAPE 1 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // extract
        assertFails("SELECT EXTRACT(DAY FROM 'a') FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // between
        assertFails("SELECT 1 BETWEEN 'a' AND 2 FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 BETWEEN 0 AND 'b' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 BETWEEN 'a' AND 'b' FROM t1")
                .hasErrorCode(TYPE_MISMATCH);

        // in
        assertFails("SELECT * FROM t1 WHERE 1 IN ('a')")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM t1 WHERE 'a' IN (1)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM t1 WHERE 'a' IN (1, 'b')")
                .hasErrorCode(TYPE_MISMATCH);

        // row type
        assertFails("SELECT t.x.f1 FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT x.f1 FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);

        // subscript on Row
        assertFails("SELECT ROW(1, 'a')[x]")
                .hasErrorCode(EXPRESSION_NOT_CONSTANT)
                .hasMessageMatching("line 1:20: Subscript expression on ROW requires a constant index");
        assertFails("SELECT ROW(1, 'a')[9999999999]")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageMatching("line 1:20: Subscript expression on ROW requires integer index, found bigint");
        assertFails("SELECT ROW(1, 'a')[-1]")
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageMatching("line 1:20: Invalid subscript index: -1. ROW indices start at 1");
        assertFails("SELECT ROW(1, 'a')[0]")
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageMatching("line 1:20: Invalid subscript index: 0. ROW indices start at 1");
        assertFails("SELECT ROW(1, 'a')[5]")
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageMatching("line 1:20: Subscript index out of bounds: 5, max value is 2");
    }

    @Test
    public void testLike()
    {
        analyze("SELECT '1' LIKE '1'");
        analyze("SELECT CAST('1' as CHAR(1)) LIKE '1'");
    }

    @Test(enabled = false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
    {
        assertFails("SELECT *")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testReferenceWithoutFrom()
    {
        assertFails("SELECT dummy")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testGroupBy()
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByEmpty()
    {
        assertFails("SELECT a FROM t1 GROUP BY ()")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
    }

    @Test
    public void testComplexExpressionInGroupingSet()
    {
        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY ROLLUP(x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:49: GROUP BY expression must be a column reference: (x + 1)\\E");
        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY CUBE(x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:47: GROUP BY expression must be a column reference: (x + 1)\\E");
        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY GROUPING SETS (x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:57: GROUP BY expression must be a column reference: (x + 1)\\E");

        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY ROLLUP(x, x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:52: GROUP BY expression must be a column reference: (x + 1)\\E");
        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY CUBE(x, x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:50: GROUP BY expression must be a column reference: (x + 1)\\E");
        assertFails("SELECT 1 FROM (VALUES 1) t(x) GROUP BY GROUPING SETS (x, x + 1)")
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageMatching("\\Qline 1:60: GROUP BY expression must be a column reference: (x + 1)\\E");
    }

    @Test
    public void testSingleGroupingSet()
    {
        // TODO: validate output
        analyze("SELECT SUM(b) FROM t1 GROUP BY ()");
        analyze("SELECT SUM(b) FROM t1 GROUP BY GROUPING SETS (())");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b))");
    }

    @Test
    public void testMultipleGroupingSetMultipleColumns()
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a, b, GROUPING SETS ((c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), ROLLUP (c, d)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), CUBE (c, d)");
    }

    @Test
    public void testAggregateWithWildcard()
    {
        assertFails("SELECT * FROM (SELECT a + 1, b FROM t1) t GROUP BY b ORDER BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("Column 1 not in GROUP BY clause");
        assertFails("SELECT * FROM (SELECT a, b FROM t1) t GROUP BY b ORDER BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("Column 't.a' not in GROUP BY clause");

        assertFails("SELECT * FROM (SELECT a, b FROM t1) GROUP BY b ORDER BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("Column 'a' not in GROUP BY clause");
        assertFails("SELECT * FROM (SELECT a + 1, b FROM t1) GROUP BY b ORDER BY 1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("Column 1 not in GROUP BY clause");
    }

    @Test
    public void testGroupByCase()
    {
        assertFails("SELECT CASE a WHEN 1 THEN 'a' ELSE 'b' END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT CASE 1 WHEN 2 THEN a ELSE 0 END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT CASE 1 WHEN 2 THEN 0 ELSE a END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);

        assertFails("SELECT CASE WHEN a = 1 THEN 'a' ELSE 'b' END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT CASE WHEN true THEN a ELSE 0 END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT CASE WHEN true THEN 0 ELSE a END, count(*) FROM t1")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
    }

    @Test
    public void testGroupingWithWrongColumnsAndNoGroupBy()
    {
        assertFails("SELECT a, SUM(b), GROUPING(a, b, c, d) FROM t1 GROUP BY GROUPING SETS ((a, b), (c))")
                .hasErrorCode(INVALID_ARGUMENTS);
        assertFails("SELECT a, SUM(b), GROUPING(a, b) FROM t1")
                .hasErrorCode(MISSING_GROUP_BY);
    }

    @Test
    public void testMismatchedUnionQueries()
    {
        assertFails("SELECT 1 UNION SELECT 'a'")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT a FROM t1 UNION SELECT 'a'")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("(SELECT 1) UNION SELECT 'a'")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1, 2 UNION SELECT 1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 'a' UNION SELECT 'b', 'c'")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("TABLE t2 UNION SELECT 'a'")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 123, 'foo' UNION ALL SELECT 'bar', 999")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageMatching(".* column 1 in UNION query has incompatible types.*");
        assertFails("SELECT 123, 123 UNION ALL SELECT 999, 'bar'")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageMatching(".* column 2 in UNION query has incompatible types.*");
    }

    @Test
    public void testUnionUnmatchedOrderByAttribute()
    {
        assertFails("TABLE t2 UNION ALL SELECT c, d FROM t1 ORDER BY c")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testGroupByComplexExpressions()
    {
        assertFails("SELECT IF(a IS NULL, 1, 0) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT IF(a IS NOT NULL, 1, 0) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT IF(CAST(a AS VARCHAR) LIKE 'a', 1, 0) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT a IN (1, 2, 3) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT 1 IN (a, 2, 3) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
    }

    @Test
    public void testNonNumericTableSamplePercentage()
    {
        assertFails("SELECT * FROM t1 TABLESAMPLE BERNOULLI ('a')")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM t1 TABLESAMPLE BERNOULLI (a + 1)")
                .hasErrorCode(EXPRESSION_NOT_CONSTANT);
    }

    @Test
    public void testTableSampleOutOfRange()
    {
        assertFails("SELECT * FROM t1 TABLESAMPLE BERNOULLI (-1)")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertFails("SELECT * FROM t1 TABLESAMPLE BERNOULLI (-101)")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testCreateTableAsColumns()
    {
        // TODO: validate output
        analyze("CREATE TABLE test(a) AS SELECT 123");
        analyze("CREATE TABLE test(a, b) AS SELECT 1, 2");
        analyze("CREATE TABLE test(a) AS (VALUES 1)");

        assertFails("CREATE TABLE test AS SELECT 123")
                .hasErrorCode(MISSING_COLUMN_NAME);
        assertFails("CREATE TABLE test AS SELECT 1 a, 2 a")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);
        assertFails("CREATE TABLE test AS SELECT null a")
                .hasErrorCode(COLUMN_TYPE_UNKNOWN);
        assertFails("CREATE TABLE test(x) AS SELECT 1, 2")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES)
                .hasLocation(1, 19);
        assertFails("CREATE TABLE test(x, y) AS SELECT 1")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES)
                .hasLocation(1, 19);
        assertFails("CREATE TABLE test(x, y) AS (VALUES 1)")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES)
                .hasLocation(1, 19);
        assertFails("CREATE TABLE test(abc, AbC) AS SELECT 1, 2")
                .hasErrorCode(DUPLICATE_COLUMN_NAME)
                .hasLocation(1, 24);
        assertFails("CREATE TABLE test(x) AS SELECT null")
                .hasErrorCode(COLUMN_TYPE_UNKNOWN)
                .hasLocation(1, 1);
        assertFails("CREATE TABLE test(x) WITH (p1 = y) AS SELECT null")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching(".*Column 'y' cannot be resolved");
        assertFails("CREATE TABLE test(x) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3') AS SELECT null")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
        assertFails("CREATE TABLE test(x) WITH (p1 = 'p1', \"p1\" = 'p2') AS SELECT null")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
    }

    @Test
    public void testCreateTable()
    {
        analyze("CREATE TABLE test (id bigint)");
        analyze("CREATE TABLE test (id bigint) WITH (p1 = 'p1')");

        assertFails("CREATE TABLE test (x bigint) WITH (p1 = y)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching(".*Column 'y' cannot be resolved");
        assertFails("CREATE TABLE test (id bigint) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
        assertFails("CREATE TABLE test (id bigint) WITH (p1 = 'p1', \"p1\" = 'p2')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
    }

    @Test
    public void testAnalyze()
    {
        analyze("ANALYZE t1");
        analyze("ANALYZE t1 WITH (p1 = 'p1')");

        assertFails("ANALYZE t1 WITH (p1 = 'p1', p2 = 2, p1 = 'p3')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
        assertFails("ANALYZE t1 WITH (p1 = 'p1', \"p1\" = 'p2')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
    }

    @Test
    public void testCreateSchema()
    {
        analyze("CREATE SCHEMA test");
        analyze("CREATE SCHEMA test WITH (p1 = 'p1')");

        assertFails("CREATE SCHEMA test WITH (p1 = y)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching(".*Column 'y' cannot be resolved");
        assertFails("CREATE SCHEMA test WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
        assertFails("CREATE SCHEMA test WITH (p1 = 'p1', \"p1\" = 'p2')")
                .hasErrorCode(DUPLICATE_PROPERTY)
                .hasMessageMatching(".* Duplicate property: p1");
    }

    @Test
    public void testCreateViewColumns()
    {
        assertFails("CREATE VIEW test AS SELECT 123")
                .hasErrorCode(MISSING_COLUMN_NAME);
        assertFails("CREATE VIEW test AS SELECT 1 a, 2 a")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);
        assertFails("CREATE VIEW test AS SELECT null a")
                .hasErrorCode(COLUMN_TYPE_UNKNOWN);
    }

    @Test
    public void testCreateRecursiveView()
    {
        assertFails("CREATE OR REPLACE VIEW v1 AS SELECT * FROM v1")
                .hasErrorCode(VIEW_IS_RECURSIVE);
    }

    @Test
    public void testExistingRecursiveView()
    {
        analyze("SELECT * FROM v1 a JOIN v1 b ON a.a = b.a");
        analyze("SELECT * FROM v1 a JOIN (SELECT * from v1) b ON a.a = b.a");
        assertFails("SELECT * FROM v5")
                .hasErrorCode(INVALID_VIEW);
    }

    @Test
    public void testShowCreateView()
    {
        analyze("SHOW CREATE VIEW v1");
        analyze("SHOW CREATE VIEW v2");

        assertFails("SHOW CREATE VIEW t1")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SHOW CREATE VIEW none")
                .hasErrorCode(TABLE_NOT_FOUND);
    }

    @Test
    public void testStaleView()
    {
        assertFails("SELECT * FROM v2")
                .hasErrorCode(VIEW_IS_STALE);
    }

    @Test
    public void testStoredViewAnalysisScoping()
    {
        // the view must not be analyzed using the query context
        analyze("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
    }

    @Test
    public void testStoredViewResolution()
    {
        // the view must be analyzed relative to its own catalog/schema
        analyze("SELECT * FROM c3.s3.v3");
    }

    @Test
    public void testQualifiedViewColumnResolution()
    {
        // it should be possible to qualify the column reference with the view name
        analyze("SELECT v1.a FROM v1");
        analyze("SELECT s1.v1.a FROM s1.v1");
        analyze("SELECT tpch.s1.v1.a FROM tpch.s1.v1");
    }

    @Test
    public void testViewWithUppercaseColumn()
    {
        analyze("SELECT * FROM v4");
    }

    @Test
    public void testUse()
    {
        assertFails("USE foo")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testNotNullInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON a.x IS NOT NULL");
    }

    @Test
    public void testIfInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON IF(a.x = 1, true, false)");
    }

    @Test
    public void testLiteral()
    {
        assertFails("SELECT TIMESTAMP '2012-10-31 01:00:00 PT'")
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testLambda()
    {
        analyze("SELECT apply(5, x -> abs(x)) from t1");
        assertFails("SELECT x -> abs(x) from t1")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testLambdaCapture()
    {
        analyze("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)");
        analyze("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)");

        // reference lambda variable of the not-immediately-enclosing lambda
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        analyze("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        analyze("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y");

        assertFails("SELECT apply(sum(x), i -> i * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        assertFails("SELECT apply(1, y -> x) FROM (VALUES (1,2)) t(x,y) GROUP BY y")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        assertFails("SELECT apply(1, y -> x.someField) FROM (VALUES (CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(x,y) GROUP BY y")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        analyze("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        analyze("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // nested lambda expression uses the same variable name
        analyze("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // illegal use of a column whose name is the same as a lambda variable name
        assertFails("SELECT apply(sum(x), x -> x * x) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        assertFails("SELECT apply(sum(x), x -> apply(x, x -> x * x)) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        // x + y within lambda should not be treated as group expression
        assertFails("SELECT apply(1, y -> x + y) FROM (VALUES (1,2)) t(x, y) GROUP BY x+y")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
        assertFails("SELECT apply(1, x -> y + transform(array[1], z -> x)[1]) FROM (VALUES (1, 2)) t(x,y) GROUP BY y + transform(array[1], z -> x)[1]")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching(".* must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testLambdaInSubqueryContext()
    {
        analyze("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)");
        analyze("SELECT apply((SELECT 10), i -> i * i)");

        // with capture
        analyze("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)");
        analyze("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)");
        analyze("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)");
    }

    @Test
    public void testLambdaWithAggregationAndGrouping()
    {
        assertFails("SELECT transform(ARRAY[1], y -> max(x)) FROM (VALUES 10) t(x)")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*");

        // use of aggregation/window function on lambda variable
        assertFails("SELECT apply(1, x -> max(x)) FROM (VALUES (1,2)) t(x,y) GROUP BY y")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*");
        assertFails("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> max(x.someField)) FROM (VALUES (1,2)) t(x,y) GROUP BY y")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*");
        assertFails("SELECT apply(1, x -> grouping(x)) FROM (VALUES (1, 2)) t(x, y) GROUP BY y")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*");
    }

    @Test
    public void testLambdaWithSubquery()
    {
        assertFails("SELECT apply(1, i -> (SELECT 3)) FROM (VALUES 1) t(x)")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageMatching(".* Lambda expression cannot contain subqueries");
        assertFails("SELECT apply(1, i -> (SELECT i)) FROM (VALUES 1) t(x)")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageMatching(".* Lambda expression cannot contain subqueries");

        // GROUP BY column captured in lambda
        analyze(
                "SELECT (SELECT apply(0, x -> x + b) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");

        // non-GROUP BY column captured in lambda
        assertFails("SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(c)) " +
                "FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:34: Subquery uses 'a' which must appear in GROUP BY clause");
        assertFails("SELECT (SELECT apply(0, x -> x + u.a) from (values 1) x(a)) " +
                "FROM t1 u GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:34: Subquery uses 'u.a' which must appear in GROUP BY clause");

        // name shadowing
        analyze("SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");
        analyze("SELECT (SELECT apply(0, a -> a + a)) FROM t1 u GROUP BY b");
    }

    @Test
    public void testLambdaWithSubqueryInOrderBy()
    {
        analyze("SELECT a FROM t1 ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT a AS output_column FROM t1 ORDER BY (SELECT apply(0, x -> x + output_column))");
        analyze("SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT count(*) AS output_column FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + output_column))");
        assertFails("SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + b))")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:71: Subquery uses 'b' which must appear in GROUP BY clause");
    }

    @Test
    public void testLambdaWithInvalidParameterCount()
    {
        assertFails("SELECT apply(5, (x, y) -> 6)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 2");
        assertFails("SELECT apply(5, (x, y, z) -> 6)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 3");
        assertFails("SELECT TRY(apply(5, (x, y) -> x + 1) / 0)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 2");
        assertFails("SELECT TRY(apply(5, (x, y, z) -> x + 1) / 0)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 3");

        assertFails("SELECT filter(ARRAY [5, 6], (x, y) -> x = 5)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 2");
        assertFails("SELECT filter(ARRAY [5, 6], (x, y, z) -> x = 5)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 3");

        assertFails("SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x) -> x = 1)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 1");
        assertFails("SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x, y, z) -> x = y + z)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3");

        assertFails("SELECT reduce(ARRAY [5, 20], 0, (s) -> s, s -> s)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 1");
        assertFails("SELECT reduce(ARRAY [5, 20], 0, (s, x, z) -> s + x, s -> s + z)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 3");

        assertFails("SELECT transform(ARRAY [5, 6], (x, y) -> x + y)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 2");
        assertFails("SELECT transform(ARRAY [5, 6], (x, y, z) -> x + y + z)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 3");

        assertFails("SELECT transform_keys(map(ARRAY[1], ARRAY [2]), k -> k)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:49: Expected a lambda that takes 2 argument\\(s\\) but got 1");
        assertFails("SELECT transform_keys(MAP(ARRAY['a'], ARRAY['b']), (k, v, x) -> k + 1)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3");

        assertFails("SELECT transform_values(map(ARRAY[1], ARRAY [2]), k -> k)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 1");
        assertFails("SELECT transform_values(map(ARRAY[1], ARRAY [2]), (k, v, x) -> k + 1)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 3");

        assertFails("SELECT zip_with(ARRAY[1], ARRAY['a'], x -> x)")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 1");
        assertFails("SELECT zip_with(ARRAY[1], ARRAY['a'], (x, y, z) -> (x, y, z))")
                .hasErrorCode(INVALID_PARAMETER_USAGE)
                .hasMessageMatching("line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 3");
    }

    @Test
    public void testInvalidDelete()
    {
        assertFails("DELETE FROM foo")
                .hasErrorCode(TABLE_NOT_FOUND);
        assertFails("DELETE FROM v1")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("DELETE FROM v1 WHERE a = 1")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testInvalidShowTables()
    {
        assertFails("SHOW TABLES FROM a.b.c")
                .hasErrorCode(SYNTAX_ERROR);

        Session session = testSessionBuilder()
                .setCatalog(null)
                .setSchema(null)
                .build();
        assertFails(session, "SHOW TABLES")
                .hasErrorCode(MISSING_CATALOG_NAME);
        assertFails(session, "SHOW TABLES FROM a")
                .hasErrorCode(MISSING_CATALOG_NAME);
        assertFails(session, "SHOW TABLES FROM c2.unknown")
                .hasErrorCode(SCHEMA_NOT_FOUND);

        session = testSessionBuilder()
                .setCatalog(SECOND_CATALOG)
                .setSchema(null)
                .build();
        assertFails(session, "SHOW TABLES")
                .hasErrorCode(MISSING_SCHEMA_NAME);
        assertFails(session, "SHOW TABLES FROM unknown")
                .hasErrorCode(SCHEMA_NOT_FOUND);
    }

    @Test
    public void testInvalidAtTimeZone()
    {
        assertFails("SELECT 'abc' AT TIME ZONE 'America/Los_Angeles'")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testValidJoinOnClause()
    {
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON TRUE");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1=1");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x=b.x AND a.y=b.y");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON NULL");
    }

    @Test
    public void testInValidJoinOnClause()
    {
        assertFails("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x + b.x")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON ROW (TRUE)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON (a.x=b.x, a.y=b.y)")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testInvalidAggregationFilter()
    {
        assertFails("SELECT sum(x) FILTER (WHERE x > 1) OVER (PARTITION BY x) FROM (VALUES (1), (2), (2), (4)) t (x)")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1)) t(x, y)")
                .hasErrorCode(FUNCTION_NOT_AGGREGATE);
        assertFails("SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1, 1)) t(x, y, z) GROUP BY z")
                .hasErrorCode(FUNCTION_NOT_AGGREGATE);
    }

    @Test
    public void testAggregationWithOrderBy()
    {
        analyze("SELECT array_agg(DISTINCT x ORDER BY x) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        analyze("SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails("SELECT array_agg(DISTINCT x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)")
                .hasErrorCode(EXPRESSION_NOT_IN_DISTINCT);
        assertFails("SELECT abs(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)")
                .hasErrorCode(FUNCTION_NOT_AGGREGATE);
        assertFails("SELECT array_agg(x ORDER BY x) FROM (VALUES MAP(ARRAY['a'], ARRAY['b'])) t(x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT 1 as a, array_agg(x ORDER BY a) FROM (VALUES (1), (2), (3)) t(x)")
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails("SELECT 1 AS c FROM (VALUES (1), (2)) t(x) ORDER BY sum(x order by c)")
                .hasErrorCode(COLUMN_NOT_FOUND);
    }

    @Test
    public void testQuantifiedComparisonExpression()
    {
        analyze("SELECT * FROM t1 WHERE t1.a <= ALL (VALUES 10, 20)");
        assertFails("SELECT * FROM t1 WHERE t1.a = ANY (SELECT 1, 2)")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT * FROM t1 WHERE t1.a = SOME (VALUES ('abc'))")
                .hasErrorCode(TYPE_MISMATCH);

        // map is not orderable
        assertFails(("SELECT map(ARRAY[1], ARRAY['hello']) < ALL (VALUES map(ARRAY[1], ARRAY['hello']))"))
                .hasErrorCode(TYPE_MISMATCH);
        // but map is comparable
        analyze(("SELECT map(ARRAY[1], ARRAY['hello']) = ALL (VALUES map(ARRAY[1], ARRAY['hello']))"));

        // HLL is neither orderable nor comparable
        assertFails("SELECT cast(NULL AS HyperLogLog) < ALL (VALUES cast(NULL AS HyperLogLog))")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT cast(NULL AS HyperLogLog) = ANY (VALUES cast(NULL AS HyperLogLog))")
                .hasErrorCode(TYPE_MISMATCH);

        // qdigest is neither orderable nor comparable
        assertFails("SELECT cast(NULL AS qdigest(double)) < ALL (VALUES cast(NULL AS qdigest(double)))")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT cast(NULL AS qdigest(double)) = ANY (VALUES cast(NULL AS qdigest(double)))")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testJoinUnnest()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN UNNEST(x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true");
    }

    @Test
    public void testJoinLateral()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN LATERAL(VALUES x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true");
    }

    @Test
    public void testNullTreatment()
    {
        assertFails("SELECT count() RESPECT NULLS OVER ()")
                .hasErrorCode(NULL_TREATMENT_NOT_ALLOWED);

        assertFails("SELECT count() IGNORE NULLS OVER ()")
                .hasErrorCode(NULL_TREATMENT_NOT_ALLOWED);

        analyze("SELECT lag(1) RESPECT NULLS OVER (ORDER BY x) FROM (VALUES 1) t(x)");
        analyze("SELECT lag(1) IGNORE NULLS OVER (ORDER BY x) FROM (VALUES 1) t(x)");
    }

    @BeforeClass
    public void setup()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CATALOG_NAME);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getTablePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getTableProperties());
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CATALOG_NAME, tpchTestCatalog.getConnector(TPCH_CATALOG_NAME).getAnalyzeProperties());

        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CATALOG_NAME));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CATALOG_NAME));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table2, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT))),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table3, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT),
                        new ColumnMetadata("x", BIGINT, null, true))),
                false));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG,
                new ConnectorTableMetadata(table4, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT))),
                false));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table5, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", BIGINT, null, true))),
                false));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table6, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", VARCHAR),
                        new ColumnMetadata("c", BIGINT),
                        new ColumnMetadata("d", BIGINT))),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table7, ImmutableList.of(
                        new ColumnMetadata("a", BIGINT),
                        new ColumnMetadata("b", DOUBLE),
                        new ColumnMetadata("c", new ArrayType(BIGINT)),
                        new ColumnMetadata("d", new ArrayType(DOUBLE)))),
                false));

        // valid view referencing table in same schema
        ConnectorViewDefinition viewData1 = new ConnectorViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v1"), viewData1, false));

        // stale view (different column type)
        ConnectorViewDefinition viewData2 = new ConnectorViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", VARCHAR.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v2"), viewData2, false));

        // view referencing table in different schema from itself and session
        ConnectorViewDefinition viewData3 = new ConnectorViewDefinition(
                "select a from t4",
                Optional.of(SECOND_CATALOG),
                Optional.of("s2"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("owner"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(THIRD_CATALOG, "s3", "v3"), viewData3, false));

        // valid view with uppercase column name
        ConnectorViewDefinition viewData4 = new ConnectorViewDefinition(
                "select A from t1",
                Optional.of("tpch"),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName("tpch", "s1", "v4"), viewData4, false));

        // recursive view referencing to itself
        ConnectorViewDefinition viewData5 = new ConnectorViewDefinition(
                "select * from v5",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("user"),
                false);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v5"), viewData5, false));

        // for identifier chain resolving tests
        catalogManager.registerCatalog(createTestingCatalog(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS_NAME));
        Type singleFieldRowType = metadata.fromSqlType("row(f1 bigint)");
        Type rowType = metadata.fromSqlType("row(f1 bigint, f2 bigint)");
        Type nestedRowType = metadata.fromSqlType("row(f1 row(f11 bigint, f12 bigint), f2 boolean)");
        Type doubleNestedRowType = metadata.fromSqlType("row(f1 row(f11 row(f111 bigint, f112 bigint), f12 boolean), f2 boolean)");

        SchemaTableName b = new SchemaTableName("a", "b");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(b, ImmutableList.of(
                        new ColumnMetadata("x", VARCHAR))),
                false));

        SchemaTableName t1 = new SchemaTableName("a", "t1");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t1, ImmutableList.of(
                        new ColumnMetadata("b", rowType))),
                false));

        SchemaTableName t2 = new SchemaTableName("a", "t2");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t2, ImmutableList.of(
                        new ColumnMetadata("a", rowType))),
                false));

        SchemaTableName t3 = new SchemaTableName("a", "t3");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t3, ImmutableList.of(
                        new ColumnMetadata("b", nestedRowType),
                        new ColumnMetadata("c", BIGINT))),
                false));

        SchemaTableName t4 = new SchemaTableName("a", "t4");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t4, ImmutableList.of(
                        new ColumnMetadata("b", doubleNestedRowType),
                        new ColumnMetadata("c", BIGINT))),
                false));

        SchemaTableName t5 = new SchemaTableName("a", "t5");
        inSetupTransaction(session -> metadata.createTable(session, CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(t5, ImmutableList.of(
                        new ColumnMetadata("b", singleFieldRowType))),
                false));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private static Analyzer createAnalyzer(Session session, Metadata metadata)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                new AllowAllAccessControl(),
                Optional.empty(),
                emptyList(),
                emptyMap(),
                WarningCollector.NOOP);
    }

    private void analyze(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION, query);
    }

    private void analyze(Session clientSession, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
                });
    }

    private PrestoExceptionAssert assertFails(Session session, @Language("SQL") String query)
    {
        return assertPrestoExceptionThrownBy(() -> analyze(session, query));
    }

    private PrestoExceptionAssert assertFails(@Language("SQL") String query)
    {
        return assertFails(CLIENT_SESSION, query);
    }

    private Catalog createTestingCatalog(String catalogName, CatalogName catalog)
    {
        CatalogName systemId = createSystemTablesCatalogName(catalog);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                catalog,
                connector,
                createInformationSchemaCatalogName(catalog),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, catalog)));
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of(
                        stringProperty("p1", "test string property", "", false),
                        integerProperty("p2", "test integer property", 0, false));
            }
        };
    }
}
