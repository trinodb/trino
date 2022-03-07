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
package io.trino.sql.analyzer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.StaticConnectorFactory;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.Statement;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.DUPLICATE_NAMED_QUERY;
import static io.trino.spi.StandardErrorCode.DUPLICATE_PROPERTY;
import static io.trino.spi.StandardErrorCode.DUPLICATE_WINDOW_NAME;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LABEL;
import static io.trino.spi.StandardErrorCode.INVALID_LIMIT_CLAUSE;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.INVALID_NAVIGATION_NESTING;
import static io.trino.spi.StandardErrorCode.INVALID_ORDER_BY;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.INVALID_PARTITION_BY;
import static io.trino.spi.StandardErrorCode.INVALID_PATTERN_RECOGNITION_FUNCTION;
import static io.trino.spi.StandardErrorCode.INVALID_PROCESSING_MODE;
import static io.trino.spi.StandardErrorCode.INVALID_RANGE;
import static io.trino.spi.StandardErrorCode.INVALID_RECURSIVE_REFERENCE;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_PATTERN;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_MEASURE;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_REFERENCE;
import static io.trino.spi.StandardErrorCode.MISMATCHED_COLUMN_ALIASES;
import static io.trino.spi.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_ALIASES;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.MISSING_GROUP_BY;
import static io.trino.spi.StandardErrorCode.MISSING_ORDER_BY;
import static io.trino.spi.StandardErrorCode.MISSING_OVER;
import static io.trino.spi.StandardErrorCode.MISSING_ROW_PATTERN;
import static io.trino.spi.StandardErrorCode.MISSING_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.MISSING_VARIABLE_DEFINITIONS;
import static io.trino.spi.StandardErrorCode.NESTED_AGGREGATION;
import static io.trino.spi.StandardErrorCode.NESTED_RECURSIVE;
import static io.trino.spi.StandardErrorCode.NESTED_ROW_PATTERN_RECOGNITION;
import static io.trino.spi.StandardErrorCode.NESTED_WINDOW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NULL_TREATMENT_NOT_ALLOWED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_HAS_NO_COLUMNS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TOO_MANY_GROUPING_SETS;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.VIEW_IS_RECURSIVE;
import static io.trino.spi.StandardErrorCode.VIEW_IS_STALE;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestAnalyzer
{
    private static final String TPCH_CATALOG = "tpch";
    private static final String SECOND_CATALOG = "c2";
    private static final String THIRD_CATALOG = "c3";
    private static final String CATALOG_FOR_IDENTIFIER_CHAIN_TESTS = "cat";
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

    private final Closer closer = Closer.create();
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private PlannerContext plannerContext;
    private TablePropertyManager tablePropertyManager;
    private AnalyzePropertyManager analyzePropertyManager;

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
    public void testNonAggregationDistinct()
    {
        assertFails("SELECT lower(DISTINCT a) FROM (VALUES('foo')) AS t1(a)")
                .hasErrorCode(FUNCTION_NOT_AGGREGATE);
        assertFails("SELECT lower(DISTINCT max(a)) FROM (VALUES('foo')) AS t1(a)")
                .hasErrorCode(FUNCTION_NOT_AGGREGATE);
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

        assertFails("SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(y) FILTER (WHERE x > 0)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageMatching("line 1:84: Invalid reference to output projection attribute from ORDER BY aggregation");
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

        // reference to outer scope relation with anonymous field
        assertFails("SELECT (SELECT outer_relation.* FROM (VALUES 1) inner_relation) FROM (values 2) outer_relation")
                .hasErrorCode(NOT_SUPPORTED);

        assertFails("SELECT t.a FROM (SELECT t.* FROM (VALUES 1) t(a))")
                .hasErrorCode(COLUMN_NOT_FOUND);
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

        // identifier chain of length 2; no ambiguity: only table match in outer scope
        analyze(CLIENT_SESSION_FOR_IDENTIFIER_CHAIN_TESTS, "SELECT (SELECT a.b.* FROM (VALUES 1) v) FROM a.b");

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
    public void testGroupByAggregation()
    {
        assertFails("SELECT x, sum(y) FROM (VALUES (1, 2)) t(x, y) GROUP BY x, sum(y)")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* GROUP BY clause cannot contain aggregations, window functions or grouping operations: .*");

        assertFails("SELECT x, sum(y) FROM (VALUES (1, 2)) t(x, y) GROUP BY 1, 2")
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessageMatching(".* GROUP BY clause cannot contain aggregations, window functions or grouping operations: .*");
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
    public void testFetchFirstInvalidRowCount()
    {
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
        // window function
        assertFails("SELECT * FROM t1 WHERE foo() over () > 1")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 GROUP BY rank() over ()")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);
        assertFails("SELECT 1 FROM (VALUES 1) HAVING count(*) OVER () > 1")
                .hasErrorCode(NESTED_WINDOW);

        // row pattern measure over window
        assertFails("SELECT * FROM t1 WHERE classy OVER ( " +
                "                                               MEASURES CLASSIFIER() AS classy " +
                "                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                               PATTERN (A+) " +
                "                                               DEFINE A AS true " +
                "                                       ) > 'X'")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);

        assertFails("SELECT * FROM t1 GROUP BY classy OVER (" +
                "                                               MEASURES CLASSIFIER() AS classy " +
                "                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                               PATTERN (A+) " +
                "                                               DEFINE A AS true " +
                "                                       )")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);

        assertFails("SELECT * FROM t1 JOIN t2 ON classy OVER (" +
                "                                               MEASURES CLASSIFIER() AS classy " +
                "                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                               PATTERN (A+) " +
                "                                               DEFINE A AS true " +
                "                                       ) = t2.a")
                .hasErrorCode(EXPRESSION_NOT_SCALAR);

        assertFails("SELECT 1 FROM (VALUES 1) HAVING classy OVER (" +
                "                                               MEASURES CLASSIFIER() AS classy " +
                "                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                               PATTERN (A+) " +
                "                                               DEFINE A AS true " +
                "                                       ) > 'X'")
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
        assertFails("SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN a PRECEDING AND UNBOUNDED FOLLOWING) FROM t1 GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        assertFails("SELECT row_number() over() as a from (values (41, 42), (-41, -42)) t(a,b) group by a+b order by a+b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("\\Qline 1:98: '(a + b)' must be an aggregate expression or appear in GROUP BY clause\\E");
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

        analyze("SELECT DISTINCT t1.* FROM t1 ORDER BY a");
        analyze("SELECT DISTINCT t1.* FROM t1 ORDER BY t1.a");
        analyze("SELECT DISTINCT t1.* AS (w, x, y, z) FROM t1 ORDER BY w");
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
                new OptimizerConfig(),
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig()))).build();
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
    public void testWindowClause()
    {
        assertFails("SELECT * FROM t1 WINDOW w AS (PARTITION BY a), w AS (PARTITION BY a)")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);

        assertFails("SELECT * FROM t1 WINDOW w AS (PARTITION BY a), w AS (ORDER BY b)")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);

        assertFails("SELECT * FROM t1 WINDOW w AS (), w1 as (), w AS (w)")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);
    }

    @Test
    public void testWindowNames()
    {
        // window names are compared using SQL identifier semantics
        assertFails("SELECT * FROM t1 WINDOW w AS (), W AS ()")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);

        analyze("SELECT * FROM t1 WINDOW w AS (), \"w\" AS ()");

        analyze("SELECT * FROM t1 WINDOW W AS (), \"w\" AS ()");

        assertFails("SELECT * FROM t1 WINDOW w AS (), \"W\" AS ()")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);

        analyze("SELECT * FROM t1 WINDOW \"W\" AS (), \"w\" AS ()");

        assertFails("SELECT * FROM t1 WINDOW \"w\" AS (), \"w\" AS ()")
                .hasErrorCode(DUPLICATE_WINDOW_NAME);

        analyze("SELECT avg(b) OVER w FROM t1 WINDOW \"W\" AS (PARTITION BY a)");

        analyze("SELECT avg(b) OVER \"W\" FROM t1 WINDOW w AS (PARTITION BY a)");

        assertFails("SELECT avg(b) OVER w FROM t1 WINDOW \"w\" AS (PARTITION BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE);

        analyze("SELECT avg(b) OVER (W ROWS CURRENT ROW) FROM t1 WINDOW \"W\" AS (PARTITION BY a)");

        assertFails("SELECT avg(b) OVER (W ROWS CURRENT ROW) FROM t1 WINDOW \"w\" AS (PARTITION BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE);
    }

    @Test
    public void testNamedWindowScope()
    {
        // window definitions with the same names are allowed in different query specifications (in this case, outer and inner query)
        analyze("SELECT * FROM (SELECT * FROM t1 WINDOW w AS (PARTITION BY a)) " +
                "WINDOW w AS (PARTITION BY a)");

        // window definition of inner query is not visible in outer query
        assertFails("SELECT avg(b) OVER w FROM (SELECT * FROM t1 WINDOW w AS (PARTITION BY a)) ")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:20: Cannot resolve WINDOW name w");

        // window definition of outer query is not visible in inner query
        assertFails("SELECT * FROM (SELECT avg(b) OVER w FROM t1) WINDOW w AS (PARTITION BY a) ")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:35: Cannot resolve WINDOW name w");

        // window definitions are visible in following window definitions of WINDOW clause
        analyze("SELECT * FROM t1 WINDOW w AS (PARTITION BY a), w1 AS (w ORDER BY b)");

        assertFails("SELECT * FROM t1 WINDOW w1 AS (w ORDER BY b), w AS (PARTITION BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:32: Cannot resolve WINDOW name w");

        // window definitions are visible in SELECT clause
        analyze("SELECT count(*) OVER w FROM t1 WINDOW w AS (PARTITION BY a)");

        // window definitions are visible in ORDER BY clause
        analyze("SELECT * FROM t1 WINDOW w AS (PARTITION BY a) ORDER BY (count(*) OVER w)");
    }

    @Test
    public void testWindowClauseWithPatternRecognition()
    {
        analyze("SELECT classy OVER w FROM t1 " +
                "                   WINDOW w AS (" +
                "                                MEASURES CLASSIFIER() AS classy " +
                "                                ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                PATTERN (A+) " +
                "                                DEFINE A AS true " +
                "                               ) ");

        analyze("SELECT classy OVER w2 FROM t1 " +
                "                   WINDOW w0 AS (PARTITION BY b), " +
                "                          w1 AS (w0 ORDER BY c), " +
                "                          w2 AS (w1 " +
                "                                 MEASURES CLASSIFIER() AS classy " +
                "                                 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                 PATTERN (A+) " +
                "                                 DEFINE A AS true " +
                "                                )");

        assertFails("SELECT classy OVER w1 FROM t1 " +
                "                   WINDOW w AS (" +
                "                                MEASURES CLASSIFIER() AS classy " +
                "                                ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                PATTERN (A+) " +
                "                                DEFINE A AS true " +
                "                               ) ")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:20: Cannot resolve WINDOW name w1");
    }

    @Test
    public void testWindowDefinition()
    {
        analyze("SELECT * FROM t1 " +
                "WINDOW " +
                "w1 AS (PARTITION BY a), " +
                "w2 AS (w1 ORDER BY b), " +
                "w3 AS (w2 RANGE c PRECEDING)," +
                "w4 AS (w1 ROWS c PRECEDING)");

        assertFails("SELECT * FROM t1 WINDOW w AS (w1 ORDER BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:31: Cannot resolve WINDOW name w1");

        assertFails("SELECT * FROM t1 WINDOW w2 AS (w1 ORDER BY a), w1 AS (PARTITION BY b)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:32: Cannot resolve WINDOW name w1");

        assertFails("SELECT * FROM t1 WINDOW w1 AS (ORDER BY a), w2 AS (w1 PARTITION BY b)")
                .hasErrorCode(INVALID_PARTITION_BY)
                .hasMessage("line 1:68: WINDOW specification with named WINDOW reference cannot specify PARTITION BY");

        assertFails("SELECT * FROM t1 WINDOW w1 AS (ORDER BY a), w2 AS (w1 ORDER BY b)")
                .hasErrorCode(INVALID_ORDER_BY)
                .hasMessage("line 1:55: Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY");

        assertFails("SELECT * FROM t1 WINDOW w1 AS (RANGE CURRENT ROW), w2 AS (w1 ORDER BY b)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:59: Cannot reference named WINDOW containing frame specification");
    }

    @Test
    public void testWindowSpecification()
    {
        // analyze PARTITION BY
        assertFails("SELECT * FROM (VALUES approx_set(1)) t(a) WINDOW w AS (PARTITION BY a)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:69: HyperLogLog is not comparable, and therefore cannot be used in window function PARTITION BY");

        // analyze ORDER BY
        assertFails("SELECT * FROM (VALUES approx_set(1)) t(a) WINDOW w AS (ORDER BY a)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:65: HyperLogLog is not orderable, and therefore cannot be used in window function ORDER BY");

        // analyze window frame
        assertFails("SELECT * FROM (VALUES 1) t(a) WINDOW w AS (RANGE UNBOUNDED FOLLOWING)")
                .hasErrorCode(INVALID_WINDOW_FRAME)
                .hasMessage("line 1:44: Window frame start cannot be UNBOUNDED FOLLOWING");

        assertFails("SELECT * FROM (VALUES 'x') t(a) WINDOW w AS (ROWS a PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:51: Window frame ROWS start value type must be exact numeric type with scale 0 (actual varchar(1))");

        assertFails("SELECT * FROM (VALUES 'x') t(a) WINDOW w AS (RANGE a PRECEDING)")
                .hasErrorCode(MISSING_ORDER_BY)
                .hasMessage("line 1:46: Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY");

        assertFails("SELECT * FROM (VALUES (1, 2, 3)) t(a, b, c) WINDOW w AS (ORDER BY a, b RANGE c PRECEDING)")
                .hasErrorCode(INVALID_ORDER_BY)
                .hasMessage("line 1:58: Window frame of type RANGE PRECEDING or FOLLOWING requires single sort item in ORDER BY (actual: 2)");

        assertFails("SELECT * FROM (VALUES 'x') t(a) WINDOW w AS (GROUPS a PRECEDING)")
                .hasErrorCode(MISSING_ORDER_BY)
                .hasMessage("line 1:46: Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY");

        assertFails("SELECT * FROM (VALUES 'x') t(a) WINDOW w AS (ROWS a PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:51: Window frame ROWS start value type must be exact numeric type with scale 0 (actual varchar(1))");

        // nested window function
        assertFails("SELECT * FROM (VALUES 1) t(a) WINDOW w AS (PARTITION BY count(a) OVER ())")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:57: Cannot nest window functions or row pattern measures inside window specification");

        assertFails("SELECT * FROM (VALUES 1) t(a) WINDOW w AS (ORDER BY count(a) OVER ())")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:53: Cannot nest window functions or row pattern measures inside window specification");

        assertFails("SELECT * FROM (VALUES 1) t(a) WINDOW w AS (ROWS count(a) OVER () PRECEDING)")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:49: Cannot nest window functions or row pattern measures inside window specification");

        // nested row pattern measure over window
        assertFails("SELECT * FROM (VALUES 1) t(a) " +
                "                       WINDOW w AS (PARTITION BY classy OVER ( " +
                "                                                               MEASURES CLASSIFIER() AS classy " +
                "                                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                               PATTERN (A+) " +
                "                                                               DEFINE A AS true " +
                "                                                              ) " +
                "                                   )")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:80: Cannot nest window functions or row pattern measures inside window specification");

        assertFails("SELECT * FROM (VALUES 1) t(a) " +
                "                       WINDOW w AS (ORDER BY classy OVER ( " +
                "                                                          MEASURES CLASSIFIER() AS classy " +
                "                                                          ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                          PATTERN (A+) " +
                "                                                          DEFINE A AS true " +
                "                                                         ) " +
                "                                   )")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:76: Cannot nest window functions or row pattern measures inside window specification");

        assertFails("SELECT * FROM (VALUES 1) t(a) " +
                "                       WINDOW w AS (ROWS r OVER ( " +
                "                                                 MEASURES A.a AS r " +
                "                                                 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                 PATTERN (A+) " +
                "                                                 DEFINE A AS true " +
                "                                                ) PRECEDING " +
                "                                   )")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:72: Cannot nest window functions or row pattern measures inside window specification");
    }

    @Test
    public void testWindowInFunctionCall()
    {
        // in SELECT expression
        analyze("SELECT max(b) OVER w FROM t1 WINDOW w AS (PARTITION BY a)");
        analyze("SELECT max(b) OVER w3 FROM t1 WINDOW w1 AS (PARTITION BY a), w2 AS (w1 ORDER BY b), w3 AS (w2 RANGE c PRECEDING)");

        assertFails("SELECT max(b) OVER w FROM t1 WINDOW w1 AS (PARTITION BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:20: Cannot resolve WINDOW name w");

        assertFails("SELECT max(c) OVER (w PARTITION BY a) FROM t1 WINDOW w AS (ORDER BY b)")
                .hasErrorCode(INVALID_PARTITION_BY)
                .hasMessage("line 1:36: WINDOW specification with named WINDOW reference cannot specify PARTITION BY");

        assertFails("SELECT max(c) OVER (w ORDER BY a) FROM t1 WINDOW w AS (ORDER BY b)")
                .hasErrorCode(INVALID_ORDER_BY)
                .hasMessage("line 1:23: Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY");

        assertFails("SELECT max(c) OVER (w ORDER BY a) FROM t1 WINDOW w AS (ROWS b PRECEDING)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:21: Cannot reference named WINDOW containing frame specification");

        analyze("SELECT max(c) OVER w FROM t1 WINDOW w AS (ROWS b PRECEDING)");

        // in ORDER BY
        analyze("SELECT * FROM t1 WINDOW w AS (PARTITION BY a) ORDER BY max(b) OVER w");
        analyze("SELECT * FROM t1 WINDOW w1 AS (PARTITION BY a), w2 AS (w1 ORDER BY b), w3 AS (w2 RANGE c PRECEDING) ORDER BY max(b) OVER w3");

        assertFails("SELECT * FROM t1 WINDOW w1 AS (PARTITION BY a) ORDER BY max(b) OVER w")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:69: Cannot resolve WINDOW name w");

        assertFails("SELECT * FROM t1 WINDOW w AS (ORDER BY b) ORDER BY max(c) OVER (w PARTITION BY a)")
                .hasErrorCode(INVALID_PARTITION_BY)
                .hasMessage("line 1:80: WINDOW specification with named WINDOW reference cannot specify PARTITION BY");

        assertFails("SELECT * FROM t1 WINDOW w AS (ORDER BY b) ORDER BY max(c) OVER (w ORDER BY a)")
                .hasErrorCode(INVALID_ORDER_BY)
                .hasMessage("line 1:67: Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY");

        assertFails("SELECT * FROM t1 WINDOW w AS (ROWS b PRECEDING) ORDER BY max(c) OVER (w ORDER BY a)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:71: Cannot reference named WINDOW containing frame specification");

        analyze("SELECT * FROM t1 WINDOW w AS (ROWS b PRECEDING) ORDER BY max(c) OVER w");

        // named window reference in subquery
        assertFails("SELECT (SELECT count(*) OVER w FROM (VALUES 2)) FROM (VALUES 1) t(x) WINDOW w AS (PARTITION BY x)")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:30: Cannot resolve WINDOW name w");

        assertFails("SELECT * FROM (VALUES 1) t(x) WINDOW w AS (PARTITION BY x) ORDER BY (SELECT count(*) OVER w FROM (VALUES 2))")
                .hasErrorCode(INVALID_WINDOW_REFERENCE)
                .hasMessage("line 1:91: Cannot resolve WINDOW name w");
    }

    @Test
    public void testWindowSpecificationWithMixedScopes()
    {
        // window ORDER BY item refers to output column (integer)
        analyze("SELECT a old_a, b a FROM (SELECT 'a', 1) t(a, b) WINDOW w AS (PARTITION BY a) ORDER BY max(b) OVER (w ORDER BY a RANGE 1 PRECEDING)");

        // window ORDER BY item refers to source column (varchar(1))
        assertFails("SELECT a old_a, b a FROM (SELECT 'a', 1) t(a, b) WINDOW w AS (PARTITION BY a ORDER BY a) ORDER BY max(b) OVER (w RANGE 1 PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:87: Window frame of type RANGE PRECEDING or FOLLOWING requires that sort item type be numeric, datetime or interval (actual: varchar(1))");
    }

    @Test
    public void testWindowWithGroupBy()
    {
        assertFails("SELECT max(a) FROM (values (1,2)) t(a,b) GROUP BY b WINDOW w AS (ORDER BY a)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:75: 'a' must be an aggregate expression or appear in GROUP BY clause");

        assertFails("SELECT max(a) FROM (values (1,2)) t(a,b) GROUP BY b WINDOW w AS (PARTITION BY a)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:79: 'a' must be an aggregate expression or appear in GROUP BY clause");

        assertFails("SELECT max(a) FROM (values (1,2)) t(a,b) GROUP BY b WINDOW w AS (ROWS a PRECEDING)")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:71: 'a' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testPatternRecognitionWithGroupBy()
    {
        analyze("SELECT m OVER( " +
                "                     MEASURES CLASSIFIER() AS m " +
                "                     ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                     PATTERN (A+) " +
                "                     DEFINE A AS true " +
                "                    ) " +
                "           FROM (VALUES (1,2)) t(a,b) GROUP BY b");

        assertFails("SELECT m OVER( " +
                "                         MEASURES CLASSIFIER() AS m " +
                "                         ROWS BETWEEN CURRENT ROW AND a FOLLOWING " +
                "                         PATTERN (A+) " +
                "                         DEFINE A AS true " +
                "                        ) " +
                "           FROM (VALUES (1,2)) t(a,b) GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:122: Window frame end must be an aggregate expression or appear in GROUP BY clause");

        assertFails("SELECT m OVER( " +
                "                         MEASURES A.a AS m " +
                "                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                         PATTERN (A+) " +
                "                         DEFINE A AS true " +
                "                        ) " +
                "           FROM (VALUES (1,2)) t(a,b) GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:50: Row pattern measure must be an aggregate expression or appear in GROUP BY clause");

        assertFails("SELECT m OVER( " +
                "                         MEASURES CLASSIFIER() AS m " +
                "                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                         PATTERN (A+) " +
                "                         DEFINE A AS A.a > 0 " +
                "                        ) " +
                "           FROM (VALUES (1,2)) t(a,b) GROUP BY b")
                .hasErrorCode(EXPRESSION_NOT_AGGREGATE)
                .hasMessageMatching("line 1:204: Row pattern variable definition must be an aggregate expression or appear in GROUP BY clause");
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
    public void testNestedMeasures()
    {
        assertFails("SELECT max(classy OVER (" +
                "                                   MEASURES CLASSIFIER() AS classy " +
                "                                   ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                   PATTERN (A+) " +
                "                                   DEFINE A AS true " +
                "                                  ) " +
                "                     ) FROM t1")
                .hasErrorCode(NESTED_WINDOW);

        assertFails("SELECT max(classy OVER (" +
                "                                   MEASURES CLASSIFIER() AS classy " +
                "                                   ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                   PATTERN (A+) " +
                "                                   DEFINE A AS true " +
                "                                  ) " +
                "                     ) OVER () FROM t1")
                .hasErrorCode(NESTED_WINDOW);

        assertFails("SELECT avg(a) OVER (PARTITION BY classy OVER (" +
                "                                                         MEASURES CLASSIFIER() AS classy " +
                "                                                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                         PATTERN (A+) " +
                "                                                         DEFINE A AS true " +
                "                                                        ) " +
                "                              ) FROM t1")
                .hasErrorCode(NESTED_WINDOW);

        assertFails("SELECT avg(a) OVER (ORDER BY classy OVER (" +
                "                                                         MEASURES CLASSIFIER() AS classy " +
                "                                                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                         PATTERN (A+) " +
                "                                                         DEFINE A AS true " +
                "                                                        ) " +
                "                              ) FROM t1")
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
    public void testWindowFrameTypeRows()
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

        assertFails("SELECT rank() OVER (ROWS 5e-1 PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS 'foo' PRECEDING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)")
                .hasErrorCode(TYPE_MISMATCH);

        analyze("SELECT rank() OVER (ROWS BETWEEN SMALLINT '1' PRECEDING AND SMALLINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ROWS BETWEEN TINYINT '1' PRECEDING AND TINYINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ROWS BETWEEN INTEGER '1' PRECEDING AND INTEGER '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ROWS BETWEEN BIGINT '1' PRECEDING AND BIGINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ROWS BETWEEN DECIMAL '1' PRECEDING AND DECIMAL '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ROWS BETWEEN CAST(1 AS decimal(38, 0)) PRECEDING AND CAST(2 AS decimal(38, 0)) FOLLOWING) FROM (VALUES 1) t(x)");
    }

    @Test
    public void testWindowFrameTypeRange()
    {
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND 2 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE 2 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND CURRENT ROW) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 5 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);

        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE 5 PRECEDING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 10 PRECEDING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 3 PRECEDING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 2 FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE CURRENT ROW) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 1 FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 10 FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)");

        // this should pass the analysis but fail during execution
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN -x PRECEDING AND 0 * x FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CAST(null AS BIGINT) PRECEDING AND CAST(null AS BIGINT) FOLLOWING) FROM (VALUES 1) t(x)");

        assertFails("SELECT array_agg(x) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(MISSING_ORDER_BY)
                .hasMessage("line 1:27: Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY");

        assertFails("SELECT array_agg(x) OVER (ORDER BY x DESC, x ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_ORDER_BY)
                .hasMessage("line 1:27: Window frame of type RANGE PRECEDING or FOLLOWING requires single sort item in ORDER BY (actual: 2)");

        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 'a') t(x)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:36: Window frame of type RANGE PRECEDING or FOLLOWING requires that sort item type be numeric, datetime or interval (actual: varchar(1))");

        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 'a' PRECEDING AND 'z' FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:52: Window frame RANGE value type (varchar(1)) not compatible with sort item type (integer)");

        assertFails("SELECT array_agg(x) OVER (ORDER BY x RANGE INTERVAL '1' day PRECEDING) FROM (VALUES INTERVAL '1' year) t(x)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:44: Window frame RANGE value type (interval day to second) not compatible with sort item type (interval year to month)");

        // window frame other than <expression> PRECEDING or <expression> FOLLOWING has no requirements regarding window ORDER BY clause
        // ORDER BY is not required
        analyze("SELECT array_agg(x) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)");
        // multiple sort keys and sort keys of types other than numeric or datetime are allowed
        analyze("SELECT array_agg(x) OVER (ORDER BY y, z RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES (1, 'text', true)) t(x, y, z)");
    }

    @Test
    public void testWindowFrameTypeGroups()
    {
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS UNBOUNDED FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS 2 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 5 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN 2 FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN 2 FOLLOWING AND CURRENT ROW) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_WINDOW_FRAME);

        assertFails("SELECT rank() OVER (GROUPS 2 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(MISSING_ORDER_BY);

        assertFails("SELECT rank() OVER (ORDER BY x GROUPS 5e-1 PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS 'foo' PRECEDING) FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 'foo' FOLLOWING) FROM (VALUES 1) t(x)")
                .hasErrorCode(TYPE_MISMATCH);

        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN SMALLINT '1' PRECEDING AND SMALLINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN TINYINT '1' PRECEDING AND TINYINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN INTEGER '1' PRECEDING AND INTEGER '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN BIGINT '1' PRECEDING AND BIGINT '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN DECIMAL '1' PRECEDING AND DECIMAL '2' FOLLOWING) FROM (VALUES 1) t(x)");
        analyze("SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CAST(1 AS decimal(38, 0)) PRECEDING AND CAST(2 AS decimal(38, 0)) FOLLOWING) FROM (VALUES 1) t(x)");
    }

    @Test
    public void testWindowFrameWithPatternRecognition()
    {
        // in-line window specification
        analyze("SELECT rank() OVER (" +
                "                           PARTITION BY x " +
                "                           ORDER BY y " +
                "                           MEASURES A.z AS last_z " +
                "                           ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                           AFTER MATCH SKIP TO NEXT ROW " +
                "                           SEEK " +
                "                           PATTERN (A B C) " +
                "                           SUBSET U = (A, B) " +
                "                           DEFINE " +
                "                               B AS false, " +
                "                               C AS true " +
                "                         ) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)");

        // window clause
        analyze("SELECT rank() OVER w FROM (VALUES (1, 2, 3)) t(x, y, z) " +
                "                       WINDOW w AS (" +
                "                           PARTITION BY x " +
                "                           ORDER BY y " +
                "                           MEASURES A.z AS last_z " +
                "                           ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                           AFTER MATCH SKIP TO NEXT ROW " +
                "                           SEEK " +
                "                           PATTERN (A B C) " +
                "                           SUBSET U = (A, B) " +
                "                           DEFINE " +
                "                               B AS false, " +
                "                               C AS true " +
                "                      ) ");
    }

    @Test
    public void testInvalidWindowFrameWithPatternRecognition()
    {
        assertFails("SELECT rank() OVER (" +
                "                           PARTITION BY x " +
                "                           ORDER BY y " +
                "                           MEASURES A.z AS last_z " +
                "                           ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                           AFTER MATCH SKIP TO NEXT ROW " +
                "                           SEEK " +
                "                           PATTERN (A B C) " +
                "                           SUBSET U = (A, B) " +
                "                         ) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_VARIABLE_DEFINITIONS)
                .hasMessage("line 1:128: Pattern recognition requires DEFINE clause");

        assertFails("SELECT rank() OVER (" +
                "                           PARTITION BY x " +
                "                           ORDER BY y " +
                "                           MEASURES A.z AS last_z " +
                "                           RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                           AFTER MATCH SKIP TO NEXT ROW " +
                "                           SEEK " +
                "                           PATTERN (A B C) " +
                "                           SUBSET U = (A, B) " +
                "                           DEFINE " +
                "                               B AS false, " +
                "                               C AS true " +
                "                         ) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(INVALID_WINDOW_FRAME)
                .hasMessage("line 1:128: Pattern recognition requires ROWS frame type");

        assertFails("SELECT rank() OVER (" +
                "                           PARTITION BY x " +
                "                           ORDER BY y " +
                "                           MEASURES A.z AS last_z " +
                "                           ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING " +
                "                           AFTER MATCH SKIP TO NEXT ROW " +
                "                           SEEK " +
                "                           PATTERN (A B C) " +
                "                           SUBSET U = (A, B) " +
                "                           DEFINE " +
                "                               B AS false, " +
                "                               C AS true " +
                "                         ) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(INVALID_WINDOW_FRAME)
                .hasMessage("line 1:128: Pattern recognition requires frame specified as BETWEEN CURRENT ROW AND ...");

        assertFails("SELECT rank() OVER ( " +
                "                               MEASURES A.z AS last_z " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_ROW_PATTERN)
                .hasMessage("line 1:53: Row pattern measures require PATTERN clause");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               AFTER MATCH SKIP TO NEXT ROW) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_ROW_PATTERN)
                .hasMessage("line 1:136: AFTER MATCH SKIP clause requires PATTERN clause");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               SEEK) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_ROW_PATTERN)
                .hasMessage("line 1:124: SEEK modifier requires PATTERN clause");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               SUBSET U = (A, B)) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_ROW_PATTERN)
                .hasMessage("line 1:131: Union variable definitions require PATTERN clause");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES (1, 2, 3)) t(x, y, z)")
                .hasErrorCode(MISSING_ROW_PATTERN)
                .hasMessage("line 1:131: Primary pattern variable definitions require PATTERN clause");
    }

    @Test
    public void testSubsetClauseInWindow()
    {
        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               SUBSET A = (C) " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:176: union pattern variable name: A is a duplicate of primary pattern variable name");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               SUBSET " +
                "                                   U = (A), " +
                "                                   U = (B) " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:255: union pattern variable name: U is declared twice");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               SUBSET U = (A, C) " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:184: subset element: C is not a primary pattern variable");
    }

    @Test
    public void testDefineClauseInWindow()
    {
        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE C AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:176: defined variable: C is not a primary pattern variable");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE " +
                "                                       A AS true, " +
                "                                       A AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:265: pattern variable with name: A is defined twice");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE A AS FINAL LAST(A.x) > 0) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:181: FINAL semantics is not supported in DEFINE clause");
    }

    @Test
    public void testRangeQuantifiersInWindow()
    {
        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A{,0}) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:134: Pattern quantifier upper bound must be greater than or equal to 1");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A{,3000000000}) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:134: Pattern quantifier upper bound must not exceed 2147483647");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A{100,1}) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_RANGE)
                .hasMessage("line 1:134: Pattern quantifier lower bound must not exceed upper bound");
    }

    @Test
    public void testAfterMatchSkipInWindow()
    {
        analyze("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               AFTER MATCH SKIP TO FIRST B " +
                "                               PATTERN (A B) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)");

        analyze("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               AFTER MATCH SKIP TO FIRST U " +
                "                               PATTERN (A B) " +
                "                               SUBSET U = (B) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               AFTER MATCH SKIP TO FIRST C " +
                "                               PATTERN (A B) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:150: C is not a primary or union pattern variable");
    }

    @Test
    public void testPatternSearchModeInWindow()
    {
        analyze("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               INITIAL " +
                "                               PATTERN (A B) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)");

        analyze("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               SEEK " +
                "                               PATTERN (A B) " +
                "                               DEFINE A AS false) " +
                "           FROM (VALUES 1) t(x)");
    }

    @Test
    public void testAnchorPatternInWindow()
    {
        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (^ A B) " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_ROW_PATTERN)
                .hasMessage("line 1:133: Anchor pattern syntax is not allowed in window");

        assertFails("SELECT rank() OVER (" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B $) " +
                "                               DEFINE B AS false) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_ROW_PATTERN)
                .hasMessage("line 1:137: Anchor pattern syntax is not allowed in window");
    }

    @Test
    public void testMatchNumberFunctionInWindow()
    {
        assertFails("SELECT rank() OVER ( " +
                "                               MEASURES 1 + MATCH_NUMBER() AS m" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS false" +
                "                              ) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:66: MATCH_NUMBER function is not supported in window");

        assertFails("SELECT rank() OVER ( " +
                "                               MEASURES B.x AS m" +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS MATCH_NUMBER() > 2" +
                "                              ) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:230: MATCH_NUMBER function is not supported in window");
    }

    @Test
    public void testLabelNamesInWindow()
    {
        // SQL identifier semantics
        analyze("SELECT rank() OVER ( " +
                "                               MEASURES " +
                "                                       \"B\".x AS m1, " +
                "                                       B.x AS m2 " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS b.x > 0" +
                "                              ) " +
                "           FROM (VALUES 1) t(x)");

        assertFails("SELECT rank() OVER ( " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS \"b\".x > 0" +
                "                              ) " +
                "           FROM (VALUES 1) t(x)")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:182: Column 'b.x' cannot be resolved");
    }

    @Test
    public void testMeasureOverWindow()
    {
        // in-line window specification
        assertFails("SELECT last_z OVER () FROM (VALUES 1) t(z) ")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");

        assertFails("SELECT last_z OVER ( " +
                "                               MEASURES CLASSIFIER() AS classy " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS true " +
                "                              ) " +
                "           FROM (VALUES 1) t(z)")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");

        assertFails("SELECT last_z OVER ( " +
                "                               MEASURES " +
                "                                        LAST(A.z) AS last_z, " +
                "                                        LAST(B.z) AS last_z " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS true " +
                "                              ) " +
                "           FROM (VALUES 1) t(z)")
                .hasErrorCode(AMBIGUOUS_NAME)
                .hasMessage("line 1:8: Measure last_z is defined more than once");

        // SQL identifier semantics
        assertFails("SELECT \"last_z\" OVER ( " +
                "                               MEASURES " +
                "                                        LAST(A.z) AS \"LAST_Z\", " +
                "                                        LAST(A.z) AS \"Last_Z\", " +
                "                                        LAST(B.z) AS last_z " +
                "                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                               PATTERN (A B) " +
                "                               DEFINE B AS true " +
                "                              ) " +
                "           FROM (VALUES 1) t(z)")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");

        // named window reference
        assertFails("SELECT last_z OVER w FROM (VALUES 1) t(z) WINDOW w AS ()")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");

        assertFails("SELECT last_z OVER w " +
                "               FROM (VALUES 1) t(z) " +
                "               WINDOW w AS ( " +
                "                            MEASURES CLASSIFIER() AS classy " +
                "                            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                            PATTERN (A B) " +
                "                            DEFINE B AS true " +
                "                           )")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");

        assertFails("SELECT last_z OVER w " +
                "               FROM (VALUES 1) t(z) " +
                "               WINDOW w AS ( " +
                "                            MEASURES " +
                "                                     LAST(A.z) AS last_z, " +
                "                                     LAST(B.z) AS last_z " +
                "                            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                            PATTERN (A B) " +
                "                            DEFINE B AS true " +
                "                           )")
                .hasErrorCode(AMBIGUOUS_NAME)
                .hasMessage("line 1:8: Measure last_z is defined more than once");

        assertFails("SELECT \"last_z\" OVER w " +
                "               FROM (VALUES 1) t(z) " +
                "               WINDOW w AS ( " +
                "                            MEASURES " +
                "                                     LAST(A.z) AS \"LAST_Z\", " +
                "                                     LAST(A.z) AS \"Last_Z\", " +
                "                                     LAST(B.z) AS last_z " +
                "                            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                            PATTERN (A B) " +
                "                            DEFINE B AS true " +
                "                           )")
                .hasErrorCode(INVALID_WINDOW_MEASURE)
                .hasMessage("line 1:8: Measure last_z is not defined in the corresponding window");
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

        // b is bigint, while a is double, coercion is possible either way
        analyze("INSERT INTO t7 (b) SELECT (a) FROM t7 ");
        analyze("INSERT INTO t7 (a) SELECT (b) FROM t7");

        // d is array of bigints, while c is array of doubles, coercion is possible either way
        analyze("INSERT INTO t7 (d) SELECT (c) FROM t7 ");
        analyze("INSERT INTO t7 (c) SELECT (d) FROM t7 ");

        analyze("INSERT INTO t7 (d) VALUES (ARRAY[null])");

        analyze("INSERT INTO t6 (d) VALUES (1), (2), (3)");
        analyze("INSERT INTO t6 (a,b,c,d) VALUES (1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'c', 3, 3), (4, 'd', 4, 4)");

        // coercion is allowed between compatible types
        analyze("INSERT INTO t8 (tinyint_column, integer_column, decimal_column, real_column) VALUES (1e0, 1e0, 1e0, 1e0)");
        analyze("INSERT INTO t8 (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (VARCHAR 'aa     ', VARCHAR 'aa     ', VARCHAR 'aa     ')");
        analyze("INSERT INTO t8 (tinyint_array_column) SELECT (bigint_array_column) FROM t8");
        analyze("INSERT INTO t8 (row_column) VALUES (ROW(ROW(1e0, VARCHAR 'aa     ')))");
        analyze("INSERT INTO t8 (date_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')");

        // coercion is not allowed between incompatible types
        assertFails("INSERT INTO t8 (integer_column) VALUES ('text')")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t8 (integer_column) VALUES (true)")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t8 (integer_column) VALUES (ROW(ROW(1e0)))")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t8 (integer_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')")
                .hasErrorCode(TYPE_MISMATCH);
        assertFails("INSERT INTO t8 (unbounded_varchar_column) VALUES (1)")
                .hasErrorCode(TYPE_MISMATCH);

        // coercion with potential loss is not allowed for nested bounded character string types
        assertFails("INSERT INTO t8 (nested_bounded_varchar_column) VALUES (ROW(ROW(CAST('aa' AS varchar(10)))))")
                .hasErrorCode(TYPE_MISMATCH);
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

        assertFails("WITH RECURSIVE a(w, x, y, z) AS (SELECT * FROM t1)," +
                "     a(a, b, c, d) AS (SELECT * FROM t1)" +
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

        assertFails("WITH RECURSIVE a(w, x, y, z) AS (SELECT * FROM t1)," +
                "     A(a, b, c, d) AS (SELECT * FROM t1)" +
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
    public void testMultipleWithListEntries()
    {
        analyze("WITH a(x) AS (SELECT 1)," +
                "   b(y) AS (SELECT x + 1 FROM a)," +
                "   c(z) AS (SELECT y * 10 FROM b)" +
                "SELECT * FROM a, b, c");

        analyze("WITH RECURSIVE a(x) AS (SELECT 1)," +
                "   b(y) AS (" +
                "       SELECT x FROM a" +
                "       UNION ALL" +
                "       SELECT y + 1 FROM b WHERE y < 3)," +
                "   c(z) AS (" +
                "       SELECT y FROM b" +
                "       UNION ALL" +
                "       SELECT z - 1 FROM c WHERE z > 0)" +
                "SELECT * FROM a, b, c");
    }

    @Test
    public void testWithQueryInvalidAliases()
    {
        assertFails("WITH a(x) AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES);

        assertFails("WITH a(x, y, z, x) AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);

        // effectively non recursive
        assertFails("WITH RECURSIVE a(x) AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES);

        assertFails("WITH RECURSIVE a(x, y, z, x) AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);

        assertFails("WITH RECURSIVE a AS (SELECT * FROM t1)" +
                "SELECT * FROM a")
                .hasErrorCode(MISSING_COLUMN_ALIASES);

        // effectively recursive
        assertFails("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES);

        assertFails("WITH RECURSIVE t(n, n) AS (" +
                "          SELECT 1, 2" +
                "          UNION ALL" +
                "          SELECT n + 2, m - 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(DUPLICATE_COLUMN_NAME);

        assertFails("WITH RECURSIVE t AS (" +
                "          SELECT 1, 2" +
                "          UNION ALL" +
                "          SELECT n + 2, m - 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(MISSING_COLUMN_ALIASES);
    }

    @Test
    public void testRecursiveBaseRelationAliasing()
    {
        // base relation anonymous
        analyze("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (4, 100))" +
                "          UNION ALL" +
                "          SELECT n + 1, m - 1 FROM t WHERE n < 5" +
                "          )" +
                "          SELECT * from t");

        // base relation aliased same as WITH query resulting table
        analyze("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (4, 100)) AS t(n, m)" +
                "          UNION ALL" +
                "          SELECT n + 1, m - 1 FROM t WHERE n < 5" +
                "          )" +
                "          SELECT * from t");

        // base relation aliased different than WITH query resulting table
        analyze("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (4, 100)) AS t1(x1, y1)" +
                "          UNION ALL" +
                "          SELECT n + 1, m - 1 FROM t WHERE n < 5" +
                "          )" +
                "          SELECT * from t");

        // same aliases for base relation and WITH query resulting table, different order
        analyze("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT * FROM (VALUES(1, 2), (4, 100)) AS t(m, n)" +
                "          UNION ALL" +
                "          SELECT n + 1, m - 1 FROM t WHERE n < 5" +
                "          )" +
                "          SELECT * from t");
    }

    @Test
    public void testColumnNumberMismatch()
    {
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2, n + 10 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH);

        assertFails("WITH RECURSIVE t(n, m) AS (" +
                "          SELECT 1, 2" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testNestedWith()
    {
        // effectively non recursive
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT * FROM (WITH RECURSIVE t2(m) AS (SELECT 1) SELECT m FROM t2)" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NESTED_RECURSIVE);

        analyze("WITH t(n) AS (" +
                "          SELECT * FROM (WITH RECURSIVE t2(m) AS (SELECT 1) SELECT m FROM t2)" +
                "          )" +
                "          SELECT * from t");

        analyze("WITH RECURSIVE t(n) AS (" +
                "          SELECT * FROM (WITH t2(m) AS (SELECT 1) SELECT m FROM t2)" +
                "          )" +
                "          SELECT * from t");

        // effectively recursive
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH RECURSIVE t2(m) AS (SELECT 4) SELECT m FROM t2 UNION SELECT n + 1 FROM t) t(n) WHERE n < 4" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NESTED_RECURSIVE);

        analyze("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t2(m) AS (SELECT 4) SELECT m FROM t2 UNION SELECT n + 1 FROM t) t(n) WHERE n < 4" +
                "          )" +
                "          SELECT * from t");
    }

    @Test
    public void testParenthesedRecursionStep()
    {
        analyze("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (((SELECT n + 2 FROM t WHERE n < 6)))" +
                "          )" +
                "          SELECT * from t");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (((TABLE t)))" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (((SELECT n + 2 FROM t WHERE n < 6) LIMIT 1))" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_LIMIT_CLAUSE);
    }

    @Test
    public void testInvalidRecursiveReference()
    {
        // WITH table name is referenced in the base relation of recursion
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1 FROM T" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        // multiple recursive references in the step relation of recursion
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT a.n + 2 FROM t AS a, t AS b WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        // step relation of recursion is not a query specification
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          TABLE T" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        // step relation of recursion is a query specification without FROM clause
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT 2 WHERE (SELECT true FROM t)" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        // step relation of recursion is a query specification with a FROM clause, but the recursive reference is not in the FROM clause
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT m FROM (VALUES 2) t2(m) WHERE (SELECT true FROM t)" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);

        // not a well-formed RECURSIVE query with recursive reference
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          INTERSECT" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE);
    }

    @Test
    public void testWithRecursiveUnsupportedClauses()
    {
        // immediate WITH clause in recursive query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          WITH t2(m) AS (SELECT 1)" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NOT_SUPPORTED);

        // immediate ORDER BY clause in recursive query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          ORDER BY 1" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NOT_SUPPORTED);

        // immediate OFFSET clause in recursive query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          OFFSET 1" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NOT_SUPPORTED);

        // immediate LIMIT clause in recursive query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          LIMIT 1" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_LIMIT_CLAUSE);

        // immediate FETCH FIRST clause in recursive query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t WHERE n < 6" +
                "          FETCH FIRST 1 ROW ONLY" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(INVALID_LIMIT_CLAUSE);
    }

    @Test
    public void testIllegalClausesInRecursiveTerm()
    {
        // recursive reference in inner source of outer join
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM (SELECT 10) u LEFT JOIN t ON true WHERE n < 6" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:114: recursive reference in right source of LEFT join");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t RIGHT JOIN (SELECT 10) u ON true WHERE n < 6" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:90: recursive reference in left source of RIGHT join");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t FULL JOIN (SELECT 10) u ON true WHERE n < 6" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:90: recursive reference in left source of FULL join");

        // recursive reference in INTERSECT
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((SELECT 10) INTERSECT ALL (TABLE t)) u(n))" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:119: recursive reference in INTERSECT ALL");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((TABLE t) INTERSECT ALL (SELECT 10)) u(n))" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:93: recursive reference in INTERSECT ALL");

        // recursive reference in EXCEPT
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((SELECT 10) EXCEPT (TABLE t)) u(n))" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:112: recursive reference in right relation of EXCEPT DISTINCT");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((SELECT 10) EXCEPT ALL (TABLE t)) u(n))" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:116: recursive reference in right relation of EXCEPT ALL");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          (SELECT n + 2 FROM ((TABLE t) EXCEPT ALL (SELECT 10)) u(n))" +
                "          )" +
                "          SELECT * FROM t")
                .hasErrorCode(INVALID_RECURSIVE_REFERENCE)
                .hasMessage("line 1:93: recursive reference in left relation of EXCEPT ALL");
    }

    @Test
    public void testRecursiveReferenceShadowing()
    {
        // table 't' in subquery refers to WITH-query defined in subquery, so it is not a recursive reference to 't' in the top-level WITH-list
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t(m) AS (SELECT 4) SELECT n + 1 FROM t)" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(COLUMN_NOT_FOUND);

        // table 't' in subquery refers to WITH-query defined in subquery, so it is not a recursive reference to 't' in the top-level WITH-list
        // the top-level WITH-query is effectively not recursive
        analyze("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t(n) AS (SELECT 4) SELECT n + 1 FROM t)" +
                "          )" +
                "          SELECT * from t");

        // the inner WITH-clause does not define a table with conflicting name 't'. Recursive reference is found in the subquery
        analyze("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t2(m) AS (SELECT 4) SELECT m FROM t2 UNION SELECT n + 1 FROM t) t(n) WHERE n < 4" +
                "          )" +
                "          SELECT * from t");

        // the inner WITH-clause defines a table with conflicting name 't'. Recursive reference in the subquery is not found even though it is before the point of shadowing
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT * FROM (WITH t2(m) AS (TABLE t), t(p) AS (SELECT 1) SELECT m + 1 FROM t2) t(n) WHERE n < 4" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TABLE_NOT_FOUND);
    }

    @Test
    public void testWithRecursiveUncoercibleTypes()
    {
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1" +
                "          UNION ALL" +
                "          SELECT BIGINT '9' FROM t WHERE n < 7" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:72: recursion step relation output type (bigint) is not coercible to recursion base relation output type (integer) at column 1");

        assertFails("WITH RECURSIVE t(n, m, p) AS (" +
                "          SELECT * FROM (VALUES(1, 2, 3))" +
                "          UNION ALL" +
                "          SELECT n + 1, BIGINT '9', BIGINT '9' FROM t WHERE n < 7" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:101: recursion step relation output type (bigint) is not coercible to recursion base relation output type (integer) at column 2");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT DECIMAL '1'" +
                "          UNION ALL" +
                "          SELECT n * 0.9 FROM t WHERE n > 0.7" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:82: recursion step relation output type (decimal(2,1)) is not coercible to recursion base relation output type (decimal(1,0)) at column 1");

        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT * FROM (VALUES('a'), ('b')) AS t(n)" +
                "          UNION ALL" +
                "          SELECT n || 'x' FROM t WHERE n < 'axxxx'" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:106: recursion step relation output type (varchar) is not coercible to recursion base relation output type (varchar(1)) at column 1");

        assertFails("WITH RECURSIVE t(n, m, o) AS (" +
                "          SELECT * FROM (VALUES(1, 2, ROW('a', 4)), (5, 6, ROW('a', 8)))" +
                "          UNION ALL" +
                "          SELECT t.o.*, ROW('a', 10) FROM t WHERE m < 3" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:132: recursion step relation output type (varchar(1)) is not coercible to recursion base relation output type (integer) at column 1");
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
        assertFails("SELECT 'abc' LIKE CHAR 'abc' FROM t1")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:19: Pattern for LIKE expression must evaluate to a varchar (actual: char(3))");
        assertFails("SELECT 'abc' LIKE 'abc' ESCAPE CHAR '#' FROM t1")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:32: Escape for LIKE expression must evaluate to a varchar (actual: char(1))");

        // extract
        assertFails("SELECT EXTRACt(DAY FROM 'a') FROM t1")
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
    public void testSetOperationNonComparableTypes()
    {
        assertFails("(VALUES approx_set(1)) INTERSECT DISTINCT (VALUES approx_set(2))")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:24: Type HyperLogLog is not comparable and therefore cannot be used in INTERSECT");

        assertFails("(VALUES approx_set(1)) INTERSECT ALL (VALUES approx_set(2))")
                .hasErrorCode(TYPE_MISMATCH);

        assertFails("(VALUES approx_set(1)) EXCEPT DISTINCT (VALUES approx_set(2))")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:24: Type HyperLogLog is not comparable and therefore cannot be used in EXCEPT");

        assertFails("(VALUES approx_set(1)) EXCEPT ALL (VALUES approx_set(2))")
                .hasErrorCode(TYPE_MISMATCH);

        assertFails("(VALUES approx_set(1)) UNION DISTINCT (VALUES approx_set(2))")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:24: Type HyperLogLog is not comparable and therefore cannot be used in UNION DISTINCT");

        analyze("(VALUES approx_set(1)) UNION ALL (VALUES approx_set(2))");
    }

    @Test
    public void testSetOperation()
    {
        analyze("VALUES (1, 'a') UNION ALL VALUES (2, 'b')");
        analyze("VALUES (1, 'a') UNION DISTINCT VALUES (2, 'b')");
        analyze("VALUES (1, 'a') INTERSECT ALL VALUES (2, 'b')");
        analyze("VALUES (1, 'a') INTERSECT DISTINCT VALUES (2, 'b')");
        analyze("VALUES (1, 'a') EXCEPT ALL VALUES (2, 'b')");
        analyze("VALUES (1, 'a') EXCEPT DISTINCT VALUES (2, 'b')");
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
        assertFails("CREATE OR REPLACE VIEW mv1 AS SELECT * FROM mv1")
                .hasErrorCode(VIEW_IS_RECURSIVE);
    }

    @Test
    public void testCreateMaterializedRecursiveView()
    {
        assertFails("CREATE OR REPLACE MATERIALIZED VIEW v1 AS SELECT * FROM v1")
                .hasErrorCode(VIEW_IS_RECURSIVE);
        assertFails("CREATE OR REPLACE MATERIALIZED VIEW mv1 AS SELECT * FROM mv1")
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

    // This test validates object resolution order (materialized view, view and table).
    // The order is arbitrary (connector should not return different object types with same name).
    // However, "SHOW CREATE" command should be consistent with how object resolution is performed
    // during table scan.
    @Test
    public void testShowCreateDuplicateNames()
    {
        analyze("SHOW CREATE MATERIALIZED VIEW table_view_and_materialized_view");
        assertFails("SHOW CREATE VIEW table_view_and_materialized_view")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Relation 'tpch.s1.table_view_and_materialized_view' is a materialized view, not a view");
        assertFails("SHOW CREATE TABLE table_view_and_materialized_view")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Relation 'tpch.s1.table_view_and_materialized_view' is a materialized view, not a table");

        analyze("SHOW CREATE VIEW table_and_view");
        assertFails("SHOW CREATE TABLE table_and_view")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Relation 'tpch.s1.table_and_view' is a view, not a table");
    }

    // This test validates object resolution order (materialized view, view and table).
    // The order is arbitrary (connector should not return different object types with same name)
    // and can be changed along with test.
    @Test
    public void testAnalysisDuplicateNames()
    {
        // Materialized view redirects to "t1"
        Analysis analysis = analyze("SELECT * FROM table_view_and_materialized_view");
        TableHandle handle = getOnlyElement(analysis.getTables());
        assertThat(((TestingTableHandle) handle.getConnectorHandle()).getTableName().getTableName()).isEqualTo("t1");

        // View redirects to "t2"
        analysis = analyze("SELECT * FROM table_and_view");
        handle = getOnlyElement(analysis.getTables());
        assertThat(((TestingTableHandle) handle.getConnectorHandle()).getTableName().getTableName()).isEqualTo("t2");
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
        // boolean
        assertFails("SELECT BOOLEAN '2'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT BOOLEAN 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // tinyint
        assertFails("SELECT TINYINT ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TINYINT '128'") // max value + 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TINYINT '-129'") // min value - 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TINYINT '12.1'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TINYINT 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // smallint
        assertFails("SELECT SMALLINT ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT SMALLINT '2147483648'") // max value + 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT SMALLINT '-2147483649'") // min value - 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT SMALLINT '12.1'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT SMALLINT 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // integer
        assertFails("SELECT INTEGER ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTEGER '2147483648'") // max value + 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTEGER '-2147483649'") // min value - 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTEGER '12.1'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTEGER 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // bigint
        assertFails("SELECT BIGINT ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT BIGINT '9223372036854775808'") // max value + 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT BIGINT '-9223372036854775809'") // min value - 1
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT BIGINT '12.1'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT BIGINT 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // real
        assertFails("SELECT REAL ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT REAL '1.2.3'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT REAL 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // double
        assertFails("SELECT DOUBLE ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DOUBLE '1.2.3'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DOUBLE 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // decimal
        assertFails("SELECT 1234567890123456789012.34567890123456789") // 39 digits, decimal point
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT 0.123456789012345678901234567890123456789") // 39 digits after "0."
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT .123456789012345678901234567890123456789") // 39 digits after "."
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL '123456789012345678901234567890123456789'") // 39 digits, no decimal point
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL '1234567890123456789012.34567890123456789'") // 39 digits, decimal point
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL '0.123456789012345678901234567890123456789'") // 39 digits after "0."
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL '.123456789012345678901234567890123456789'") // 39 digits after "."
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DECIMAL 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // date
        assertFails("SELECT DATE '20220101'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DATE 'a'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DATE 'today'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT DATE '2022-01-01 UTC'")
                .hasErrorCode(INVALID_LITERAL);

        // time
        assertFails("SELECT TIME ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIME '12'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIME '1234567'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIME 'a'")
                .hasErrorCode(INVALID_LITERAL);

        // timestamp
        assertFails("SELECT TIMESTAMP ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIMESTAMP '2012-10-31 01:00:00 PT'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIMESTAMP 'a'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT TIMESTAMP 'now'")
                .hasErrorCode(INVALID_LITERAL);

        // interval
        assertFails("SELECT INTERVAL 'a' DAY TO SECOND")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTERVAL '12.1' DAY TO SECOND")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTERVAL '12' YEAR TO DAY")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT INTERVAL '12' SECOND TO MINUTE")
                .hasErrorCode(INVALID_LITERAL);

        // json
        assertFails("SELECT JSON ''")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{}{'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{} \"a\"'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{}{'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{} \"a\"'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{}{abc'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON '{}abc'")
                .hasErrorCode(INVALID_LITERAL);
        assertFails("SELECT JSON ''")
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
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .build();
        assertFails(session, "SHOW TABLES")
                .hasErrorCode(MISSING_CATALOG_NAME);
        assertFails(session, "SHOW TABLES FROM a")
                .hasErrorCode(MISSING_CATALOG_NAME);
        assertFails(session, "SHOW TABLES FROM c2.unknown")
                .hasErrorCode(SCHEMA_NOT_FOUND);

        session = testSessionBuilder()
                .setCatalog(SECOND_CATALOG)
                .setSchema(Optional.empty())
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
                .hasErrorCode(TYPE_MISMATCH);
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

        // complex row with non-comparable field
        assertFails("SELECT ROW(cast(NULL AS HyperLogLog), 1) = ANY (VALUES ROW(cast(NULL AS HyperLogLog), 1))")
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
        // Lateral references are only allowed in INNER and LEFT join.
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN UNNEST(x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true");
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        // Join involving UNNEST only supported without condition (cross join) or with condition ON TRUE
        analyze("SELECT * FROM (VALUES 1), UNNEST(array[2])");
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT JOIN UNNEST(x) b(x) USING (x)")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT JOIN UNNEST(x) ON 1 = 1")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT JOIN UNNEST(x) ON false")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testJoinLateral()
    {
        // Lateral references are only allowed in INNER and LEFT join.
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN LATERAL(VALUES x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true");
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        assertFails("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);
        // FULL join involving LATERAL relation only supported with condition ON TRUE
        analyze("SELECT * FROM (VALUES 1) FULL OUTER JOIN LATERAL(VALUES 2) ON true");
        assertFails("SELECT * FROM (VALUES 1) a(x) FULL OUTER JOIN LATERAL(VALUES 2) b(x) USING (x)")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT * FROM (VALUES 1) FULL OUTER JOIN LATERAL(VALUES 2) ON 1 = 1")
                .hasErrorCode(NOT_SUPPORTED);
        assertFails("SELECT * FROM (VALUES 1) FULL OUTER JOIN LATERAL(VALUES 2) ON false")
                .hasErrorCode(NOT_SUPPORTED);
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

    @Test
    public void testCreateOrReplaceMaterializedView()
    {
        assertFails("CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT * FROM tab1")
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testValues()
    {
        assertFails("VALUES (1, 2, 3), (1, 2)")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:1: Values rows have mismatched sizes: 3 vs 2");

        assertFails("VALUES (1, 2), 1")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:1: Values rows have mismatched sizes: 2 vs 1");

        assertFails("VALUES (1, 2), CAST(ROW(1, 2, 3) AS row(bigint, bigint, bigint))")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:1: Values rows have mismatched sizes: 2 vs 3");

        assertFails("VALUES (1, 2), ('a', 'b')")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:1: Values rows have mismatched types: row(integer, integer) vs row(varchar(1), varchar(1))");

        analyze("VALUES 'a', ('a'), ROW('a'), CAST(ROW('a') AS row(char(5)))");
    }

    // TEST ROW PATTERN RECOGNITION: MATCH_RECOGNIZE CLAUSE
    @Test
    public void testInputColumnNames()
    {
        // ambiguous columns in row pattern recognition input table
        String query = "SELECT * " +
                "          FROM (VALUES (1, 2, 3)) Ticker(%s) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY y " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) AS M";
        assertFails(format(query, "x, X, y"))
                .hasErrorCode(AMBIGUOUS_NAME);

        // TODO This should not fail according to SQL identifier semantics.
        //  Fix column name resolution so that fields contain canonical name.
        assertFails(format(query, "\"x\", \"X\", y"))
                .hasErrorCode(AMBIGUOUS_NAME);

        assertFails(format(query, "x, \"X\", y"))
                .hasErrorCode(AMBIGUOUS_NAME);

        // using original column names from input table
        analyze("SELECT a " +
                "          FROM t1 " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY a " +
                "                   ORDER BY b " +
                "                   MEASURES X.d AS m " +
                "                   PATTERN (X Y+) " +
                "                   DEFINE Y AS Y.c > 5 " +
                "                 ) AS M");

        // column aliases of input table are visible inside MATCH_RECOGNIZE clause and in its output
        analyze("SELECT q " +
                "          FROM t1 AS t(q, r, s, t) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY q " +
                "                   ORDER BY r " +
                "                   MEASURES X.t AS m " +
                "                   PATTERN (X Y+) " +
                "                   DEFINE Y AS Y.s > 5 " +
                "                 ) AS M");

        assertFails("SELECT * " +
                "          FROM t1 AS t(q, r, s, t)" +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY a " +
                "                   PATTERN (X Y+) " +
                "                   DEFINE Y AS true " +
                "                 ) AS M")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:111: Column a is not present in the input relation");

        assertFails("SELECT * " +
                "          FROM t1 AS t(q, r, s, t)" +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY q " +
                "                   PATTERN (X Y+) " +
                "                   DEFINE Y AS Y.a > 5 " +
                "                 ) AS M")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:178: Column a prefixed with label Y cannot be resolved");

        // label-prefixed column references are recognized case-insensitive
        analyze("SELECT * " +
                "          FROM t1 AS t(q, r, S, T) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES " +
                "                       X.Q AS m1, " +
                "                       X.r AS m2" +
                "                   PATTERN (X Y+) " +
                "                   DEFINE " +
                "                       X AS Y.S > 5, " +
                "                       Y AS Y.t < 5 " +
                "                 ) AS M");
    }

    @Test
    public void testInputTableNameVisibility()
    {
        // the input table name is 'Ticker'
        String query = "SELECT %s " +
                "          FROM (VALUES (1, 2, 3)) Ticker(x, y, z) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY y " +
                "                   MEASURES CLASSIFIER() AS Measure " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) %s";

        // input table name is not visible in SELECT clause when output name is not specified
        assertFails(format(query, "Ticker.Measure", ""))
                .hasErrorCode(COLUMN_NOT_FOUND);
        assertFails(format(query, "Ticker.*", ""))
                .hasErrorCode(TABLE_NOT_FOUND);
        assertFails(format(query, "Ticker.y", ""))
                .hasErrorCode(COLUMN_NOT_FOUND);
        // input table name is not visible in SELECT clause when output name is specified
        assertFails(format(query, "Ticker.Measure", "AS M"))
                .hasErrorCode(COLUMN_NOT_FOUND);

        // input table name is visible in PARTITION BY and ORDER BY clauses
        analyze("SELECT * " +
                "          FROM (VALUES (1, 2, 3)) Ticker(x, y, z) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Ticker.x  " +
                "                   ORDER BY Ticker.y  " +
                "                   MEASURES CLASSIFIER() AS Measure " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ");

        // input table name is not visible in MEASURES and DEFINE clauses
        query = "SELECT * " +
                "          FROM (VALUES (1, 2, 3)) Ticker(x, y, z) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY Ticker.x " +
                "                   MEASURES %s " +
                "                   PATTERN (A B+) " +
                "                   DEFINE %s " +
                "                 ) ";

        assertFails(format(query, "A.Ticker.x AS Measure", "B AS true"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:164: Column ticker.x prefixed with label A cannot be resolved");
        assertFails(format(query, "Ticker.A.x AS Measure", "B AS true"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:164: Column 'ticker.a.x' cannot be resolved");
        assertFails(format(query, "1 AS Measure", "B AS Ticker.x > 0"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:242: Column 'ticker.x' cannot be resolved");

        // for non-aliased input relation, the same rules apply to its original name
        analyze("SELECT * " +
                "          FROM t1 " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY t1.a " +
                "                   ORDER BY t1.b " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                  ) ");

        assertFails(format(query, "A.t1.x AS Measure", "B AS true"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:164: Column t1.x prefixed with label A cannot be resolved");
        assertFails(format(query, "t1.A.x AS Measure", "B AS true"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:164: Column 't1.a.x' cannot be resolved");
        assertFails(format(query, "1 AS Measure", "B AS t1.x > 0"))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:242: Column 't1.x' cannot be resolved");
    }

    @Test
    public void testOutputTableNameAndAliases()
    {
        String query = "SELECT %s " +
                "          FROM (VALUES (1, 2, 3)) Ticker(x, y, z) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY y " +
                "                   MEASURES CLASSIFIER() AS Measure " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) %s";

        analyze(format(query, "M.Measure", "AS M"));

        assertFails(format(query, "M.renamed", "AS M (renamed)"))
                .hasErrorCode(MISMATCHED_COLUMN_ALIASES)
                .hasMessage("line 1:33: Column alias list has 1 entries but 'M' has 2 columns available");

        assertFails(format(query, "M.Measure", "AS M (partition, renamed)"))
                .hasErrorCode(COLUMN_NOT_FOUND);

        analyze(format(query, "M.renamed", "AS M (partition, renamed)"));
    }

    @Test
    public void testPartitionBy()
    {
        // PARTITION BY expressions must be input columns
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x + 1 " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);

        assertFails("SELECT * " +
                "          FROM (VALUES approx_set(1)) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testOrderBy()
    {
        // ORDER BY expressions must be input columns
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY x + 1 " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_COLUMN_REFERENCE);

        assertFails("SELECT * " +
                "          FROM (VALUES approx_set(1)) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testLabelNames()
    {
        // pattern variables names (labels) are compared using SQL identifier semantics
        String query = "SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   %s " + // PATTERN
                "                   %s " + // DEFINE
                "                 ) ";

        analyze(format(query, "PATTERN(A)", "DEFINE a AS true"));
        analyze(format(query, "PATTERN(a)", "DEFINE A AS true"));
        analyze(format(query, "PATTERN(\"A\")", "DEFINE a AS true"));

        assertFails(format(query, "PATTERN(a)", "DEFINE \"a\" AS true"))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:171: defined variable: \"a\" is not a primary pattern variable");

        assertFails(format(query, "PATTERN(A)", "DEFINE \"a\" AS true"))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:171: defined variable: \"a\" is not a primary pattern variable");

        analyze(format(query, "PATTERN(A \"a\")", "DEFINE A AS true, \"a\" as false"));
        analyze(format(query, "PATTERN(A \"a\")", "DEFINE a AS true, \"a\" as false"));

        assertFails(format(query, "PATTERN(A \"a\")", "DEFINE A AS true, a as false"))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:186: pattern variable with name: a is defined twice");

        assertFails(format(query, "PATTERN(A \"a\")", "DEFINE \"a\" AS true, \"a\" as false"))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:188: pattern variable with name: \"a\" is defined twice");

        // delimited label names identical to anchor pattern tokens '^' and '$'
        analyze("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   AFTER MATCH SKIP TO LAST \"^\" " +
                "                   PATTERN (A B+ \"$\") " +
                "                   SUBSET \"^\" = (A, \"$\") " +
                "                   DEFINE \"$\" AS true " +
                "                 ) ");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (b, \"a\") " +
                "                   DEFINE A AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:183: subset element: \"a\" is not a primary pattern variable");

        analyze("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   AFTER MATCH SKIP TO LAST \"A\" " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (a, b) " +
                "                   DEFINE A AS true " +
                "                 ) ");

        analyze("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES " +
                "                       LAST(A.x) AS uppercase_measure, " +
                "                       LAST(a.x) AS lowercase_measure, " +
                "                       LAST(\"A\".x) AS delimited_measure " +
                "                   PATTERN (A+) " +
                "                   DEFINE A AS true " +
                "                 ) ");
    }

    @Test
    public void testLabelNamesInExpressions()
    {
        analyze("SELECT M.Measure1, M.Measure2, M.Measure3, M.Measure4, M.Measure5, M.Measure6 " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES " +
                "                       CLASSIFIER(A) AS Measure1, " +
                "                       LAST(a.Tradeday) AS Measure2, " +
                "                       FIRST(\"A\".Price) AS Measure3, " +
                "                       B.Symbol + 4 AS Measure4, " +
                "                       b.Symbol + 5 AS Measure5, " +
                "                       lower(CLASSIFIER(\"B\")) AS Measure6 " +
                "                   PATTERN (a B+) " +
                "                   DEFINE B AS true " +
                "                ) AS M");
    }

    @Test
    public void testSubsetClause()
    {
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   SUBSET A = (B) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:175: union pattern variable name: A is a duplicate of primary pattern variable name");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+ C) " +
                "                   SUBSET S = (B), " +
                "                          S = (C) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:212: union pattern variable name: S is declared twice");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+ C) " +
                "                   SUBSET S = (B, X) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:185: subset element: X is not a primary pattern variable");
    }

    @Test
    public void testDefineClause()
    {
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true, " +
                "                          X AS false " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:212: defined variable: X is not a primary pattern variable");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true, " +
                "                          B AS false " +
                "                 ) ")
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:212: pattern variable with name: B is defined twice");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS A.x " +
                "                 ) ")
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:180: Expression defining a label must be boolean (actual type: integer)");

        // FINAL semantics is not supported in DEFINE clause. RUNNING semantics is supported
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS FINAL LAST(A.x) > 5 " +
                "                 ) ")
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:180: FINAL semantics is not supported in DEFINE clause");

        analyze("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS RUNNING LAST(A.x) > 5 " +
                "                 ) ");
    }

    @Test
    public void testNoInitialOrSeek()
    {
        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   INITIAL PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:134: Pattern search modifier: INITIAL is not allowed in MATCH_RECOGNIZE clause");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   SEEK PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:134: Pattern search modifier: SEEK is not allowed in MATCH_RECOGNIZE clause");
    }

    @Test
    public void testPatternExclusions()
    {
        String query = "SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   %s " +
                "                   PATTERN ({- A -} B+) " +
                "                   DEFINE B AS true " +
                "                 ) ";

        analyze(format(query, ""));
        analyze(format(query, "ONE ROW PER MATCH"));
        analyze(format(query, "ALL ROWS PER MATCH"));
        analyze(format(query, "ALL ROWS PER MATCH SHOW EMPTY MATCHES"));
        analyze(format(query, "ALL ROWS PER MATCH OMIT EMPTY MATCHES"));

        assertFails(format(query, "ALL ROWS PER MATCH WITH UNMATCHED ROWS"))
                .hasErrorCode(INVALID_ROW_PATTERN)
                .hasMessage("line 1:201: Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified");
    }

    @Test
    public void testPatternQuantifiers()
    {
        String query = "SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   PATTERN (A %s) " +
                "                   DEFINE A AS true " +
                "                 ) ";

        analyze(format(query, "*"));
        analyze(format(query, "*?"));
        analyze(format(query, "+"));
        analyze(format(query, "+?"));
        analyze(format(query, "?"));
        analyze(format(query, "??"));
        analyze(format(query, "{,}"));
        analyze(format(query, "{5}"));
        assertFails(format(query, "{0}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier upper bound must be greater than or equal to 1");
        assertFails(format(query, "{3000000000}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier lower bound must not exceed 2147483647");
        analyze(format(query, "{5,}"));
        analyze(format(query, "{0,}"));
        assertFails(format(query, "{3000000000,}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier lower bound must not exceed 2147483647");
        analyze(format(query, "{0,5}"));
        assertFails(format(query, "{0,0}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier upper bound must be greater than or equal to 1");
        assertFails(format(query, "{5, 3000000000}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier upper bound must not exceed 2147483647");
        assertFails(format(query, "{5,1}"))
                .hasErrorCode(INVALID_RANGE)
                .hasMessage("line 1:145: Pattern quantifier lower bound must not exceed upper bound");
        analyze(format(query, "{,5}"));
        assertFails(format(query, "{,0}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier upper bound must be greater than or equal to 1");
        assertFails(format(query, "{,3000000000}"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:145: Pattern quantifier upper bound must not exceed 2147483647");
    }

    @Test
    public void testAfterMatchSkipClause()
    {
        String query = "SELECT * " +
                "          FROM (VALUES 1) Ticker(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PARTITION BY x " +
                "                   %s " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ";

        analyze(format(query, ""));
        analyze(format(query, "AFTER MATCH SKIP PAST LAST ROW"));
        analyze(format(query, "AFTER MATCH SKIP TO NEXT ROW"));
        analyze(format(query, "AFTER MATCH SKIP TO FIRST B"));
        analyze(format(query, "AFTER MATCH SKIP TO LAST B"));
        analyze(format(query, "AFTER MATCH SKIP TO B"));

        assertFails(format(query, "AFTER MATCH SKIP TO LAST \"^\""))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:159: \"^\" is not a primary or union pattern variable");

        assertFails(format(query, "AFTER MATCH SKIP TO LAST X"))
                .hasErrorCode(INVALID_LABEL)
                .hasMessage("line 1:159: X is not a primary or union pattern variable");
    }

    @Test
    public void testNestedMatchRecognize()
    {
        // in DEFINE clause of another MATCH_RECOGNIZE
        assertFails("SELECT * " +
                "          FROM (VALUES 1) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES CLASSIFIER() AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS EXISTS " +
                "                                   (SELECT c FROM (VALUES 2) t(a)" +
                "                                                    MATCH_RECOGNIZE ( " +
                "                                                      MEASURES CLASSIFIER() AS c " +
                "                                                      PATTERN (X*) " +
                "                                                      DEFINE X AS true " +
                "                                                    ) t2 " +
                "                                    ) " +
                "                 ) ")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:239: nested row pattern recognition in row pattern recognition");

        // in MEASURES clause of another MATCH_RECOGNIZE
        assertFails("SELECT * " +
                "          FROM (VALUES 1) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES EXISTS " +
                "                                (SELECT c FROM (VALUES 2) t(a)" +
                "                                                 MATCH_RECOGNIZE ( " +
                "                                                   MEASURES CLASSIFIER() AS c " +
                "                                                   PATTERN (X*) " +
                "                                                   DEFINE X AS true " +
                "                                                 ) t2 " +
                "                                 ) AS c" +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true" +
                "                ) ")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:153: nested row pattern recognition in row pattern recognition");

        // in DEFINE clause of window frame with pattern recognition
        assertFails("SELECT m OVER( " +
                "                     MEASURES CLASSIFIER() AS m" +
                "                     ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                     PATTERN (A+) " +
                "                     DEFINE A AS EXISTS " +
                "                                (SELECT c FROM (VALUES 2) t(a)" +
                "                                                 MATCH_RECOGNIZE ( " +
                "                                                   MEASURES CLASSIFIER() AS c " +
                "                                                   PATTERN (X*) " +
                "                                                   DEFINE X AS true " +
                "                                                 ) t2 " +
                "                                 ) " +
                "                    ) FROM t1")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:246: nested row pattern recognition in row pattern recognition");

        // in MEASURES clause of window frame with pattern recognition
        assertFails("SELECT m OVER( " +
                "                     MEASURES EXISTS " +
                "                                (SELECT c FROM (VALUES 2) t(a)" +
                "                                                 MATCH_RECOGNIZE ( " +
                "                                                   MEASURES CLASSIFIER() AS c " +
                "                                                   PATTERN (X*) " +
                "                                                   DEFINE X AS true " +
                "                                                 ) t2 " +
                "                                 ) AS m" +
                "                     ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                     PATTERN (A+) " +
                "                     DEFINE A AS true " +
                "                    ) FROM t1")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:100: nested row pattern recognition in row pattern recognition");

        // in RECURSIVE query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1 " +
                "          UNION ALL" +
                "          SELECT n + 2 FROM t MATCH_RECOGNIZE ( " +
                "                                MEASURES CLASSIFIER() AS c " +
                "                                PATTERN (X*) " +
                "                                DEFINE X AS true " +
                "                              ) " +
                "          WHERE n < 6" +
                "          )" +
                "          SELECT * from t")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:91: nested row pattern recognition in recursive query");
    }

    @Test
    public void testNestedPatternRecognitionInWindow()
    {
        // in DEFINE clause of MATCH_RECOGNIZE
        assertFails("SELECT * " +
                "          FROM (VALUES 1) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES CLASSIFIER() AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS classy OVER ( " +
                "                                            MEASURES CLASSIFIER() AS classy " +
                "                                            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                            PATTERN (X+) " +
                "                                            DEFINE X AS true " +
                "                                           ) > 'Z'" +
                "                 ) ")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:410: nested row pattern recognition in row pattern recognition");

        // in MEASURES clause of MATCH_RECOGNIZE
        assertFails("SELECT * " +
                "          FROM (VALUES 1) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES classy OVER ( " +
                "                                         MEASURES CLASSIFIER() AS classy " +
                "                                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                         PATTERN (X+) " +
                "                                         DEFINE X AS true " +
                "                                        ) > 'Z' AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:318: nested row pattern recognition in row pattern recognition");

        // in DEFINE clause of window frame with pattern recognition
        assertFails("SELECT m OVER( " +
                "                         MEASURES CLASSIFIER() AS m" +
                "                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                         PATTERN (A+) " +
                "                         DEFINE A AS classy OVER ( " +
                "                                                  MEASURES CLASSIFIER() AS classy " +
                "                                                  ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                                  PATTERN (X+) " +
                "                                                  DEFINE X AS true " +
                "                                                 ) > 'Z'" +
                "                        ) FROM t1")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:208: Cannot nest window functions or row pattern measures inside window specification");

        // in MEASURES clause of window frame with pattern recognition
        assertFails("SELECT m OVER( " +
                "                         MEASURES classy OVER ( " +
                "                                               MEASURES CLASSIFIER() AS classy " +
                "                                               ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                                               PATTERN (X+) " +
                "                                               DEFINE X AS true " +
                "                                              ) > 'Z' AS m" +
                "                         ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                         PATTERN (A+) " +
                "                         DEFINE A AS true" +
                "                        ) FROM t1")
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:50: Cannot nest window functions or row pattern measures inside window specification");

        // in RECURSIVE query
        assertFails("WITH RECURSIVE t(n) AS (" +
                "          SELECT 1 " +
                "          UNION ALL" +
                "          SELECT n + m OVER w FROM t " +
                "               WHERE n < 6 " +
                "               WINDOW w AS ( " +
                "                            MEASURES X.n AS m " +
                "                            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING " +
                "                            PATTERN (X*) " +
                "                            DEFINE X AS true " +
                "                           ) " +
                "          ) " +
                "          SELECT * from t")
                .hasErrorCode(NESTED_ROW_PATTERN_RECOGNITION)
                .hasMessage("line 1:308: nested row pattern recognition in recursive query");
    }

    @Test
    public void testCorrelation()
    {
        // outer query references are not allowed in DEFINE clause
        assertFails("SELECT (SELECT * " +
                "                   FROM (VALUES 1) Ticker(x) " +
                "                         MATCH_RECOGNIZE ( " +
                "                           PARTITION BY x " +
                "                           PATTERN (A B+) " +
                "                           DEFINE B AS t1.a > PREV(B.x) " +
                "                         ) " +
                "                  ) FROM t1")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:229: Column 't1.a' cannot be resolved");

        // outer query references are not allowed in MEASURES clause
        assertFails("SELECT (SELECT * " +
                "                   FROM (VALUES 1) Ticker(x) " +
                "                         MATCH_RECOGNIZE ( " +
                "                           MEASURES t1.a - PREV(B.x) AS m " +
                "                           PATTERN (A B+) " +
                "                           DEFINE B AS true " +
                "                         ) " +
                "                  ) FROM t1")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:142: Column 't1.a' cannot be resolved");

        // in this example, "B.x" is not an outer reference but a label dereference
        analyze("SELECT (SELECT * " +
                "                   FROM (VALUES 1) Ticker(x) " +
                "                         MATCH_RECOGNIZE ( " +
                "                           MEASURES FIRST(B.x) AS m " +
                "                           PATTERN (A B+) " +
                "                           DEFINE B AS true " +
                "                         ) " +
                "                  ) FROM (VALUES 2) b");
    }

    @Test
    public void testSubqueries()
    {
        // subqueries are supported in MEASURES and DEFINE clauses
        analyze("SELECT * " +
                "          FROM (VALUES 1) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES (SELECT 1) AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS (SELECT true) " +
                "                 ) ");

        // subqueries must not use pattern variables
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES (SELECT A.x) AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:112: Column 'a.x' cannot be resolved");

        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS (SELECT A.x > 5) " +
                "                 ) ")
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:184: Column 'a.x' cannot be resolved");

        // subqueries must not use outer scope references (in this case, reference to row pattern input table)
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS (SELECT t.x > 5)" +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:184: Reference to column 't.x' from outer scope not allowed in this context");
    }

    @Test
    public void testInPredicateWithSubquery()
    {
        // value can use plain column references
        analyze(("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS 5 + x in (SELECT 1)" +
                "                 ) "));

        // value must not use pattern variables
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS A.x in (SELECT 1)" +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:176: IN-PREDICATE with labeled column reference is not yet supported");

        // value must not use navigations
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS LAST(x) in (SELECT 1)" +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:176: IN-PREDICATE with last function is not yet supported");

        // value must not use CLASSIFIER()
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS CLASSIFIER() in (SELECT 1)" +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:176: IN-PREDICATE with classifier function is not yet supported");

        // value must not use MATCH_NUMBER()
        assertFails("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS MATCH_NUMBER() in (SELECT 1)" +
                "                 ) ")
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:176: IN-PREDICATE with match_number function is not yet supported");
    }

    @Test
    public void testInPredicateWithoutSubquery()
    {
        // value and value list can use plain column references
        analyze(("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS 5 + x in (1, 2, x)" +
                "                 ) "));

        // value and value list can use pattern variables
        analyze("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS A.x in (1, 2, B.x)" +
                "                 ) ");

        // value and value list can use navigations
        analyze("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS LAST(x) in (1, 2, FIRST(x))" +
                "                 ) ");

        // value and value list can use CLASSIFIER()
        analyze("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS CLASSIFIER(A) in ('A', 'B', CLASSIFIER(B))" +
                "                 ) ");

        // valu and value liste can use MATCH_NUMBER()
        analyze("SELECT * " +
                "          FROM (VALUES 1) t(x) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES 1 AS c " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS MATCH_NUMBER() in (1, 2, MATCH_NUMBER())" +
                "                 ) ");
    }

    @Test
    public void testPatternRecognitionConcatenation()
    {
        analyze("SELECT * " +
                "           FROM (SELECT * " +
                "                 FROM (VALUES 1) " +
                "                       MATCH_RECOGNIZE ( " +
                "                         MEASURES 1 AS c" +
                "                         PATTERN (A B+) " +
                "                         DEFINE B AS true" +
                "                       ) " +
                "                 ) MATCH_RECOGNIZE ( " +
                "                     MEASURES 1 AS c" +
                "                     PATTERN (A B+) " +
                "                     DEFINE B AS true" +
                "                   ) ");
    }

    @Test
    public void testNoOutputColumns()
    {
        assertFails("SELECT 1 " +
                "          FROM (VALUES 2) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS true " +
                "                 ) ")
                .hasErrorCode(TABLE_HAS_NO_COLUMNS)
                .hasMessage("line 1:25: pattern recognition output table has no columns");
    }

    @Test
    public void testLambdaInPatternRecognition()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (ARRAY[1]), (ARRAY[2])) Ticker(Value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS %s " +
                "                ) AS M";

        assertFails(format(query, "transform(A.Value, x -> x + 100)", "true"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:161: Lambda expression in pattern recognition context is not yet supported");
        assertFails(format(query, "true", "transform(A.Value, x -> x + 100) = ARRAY[50]"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:242: Lambda expression in pattern recognition context is not yet supported");
    }

    @Test
    public void testTryInPatternRecognition()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (ARRAY[1]), (ARRAY[2])) Ticker(Value) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   DEFINE B AS %s " +
                "                ) AS M";

        assertFails(format(query, "TRY(1)", "true"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:142: TRY expression in pattern recognition context is not yet supported");
        assertFails(format(query, "sum(TRY(1))", "true"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:146: TRY expression in pattern recognition context is not yet supported");
        assertFails(format(query, "true", "TRY(1) = 1"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:223: TRY expression in pattern recognition context is not yet supported");
        assertFails(format(query, "true", "sum(TRY(1)) = 2"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:227: TRY expression in pattern recognition context is not yet supported");
    }

    @Test
    public void testRowPatternRecognitionFunctions()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS %s " +
                "                 ) AS M";

        // test illegal clauses in MEASURES
        String define = "true";
        assertFails(format(query, "LAST(Tradeday) OVER ()", define))
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:195: Cannot nest window functions or row pattern measures inside pattern recognition expressions");

        assertFails(format(query, "LAST(Tradeday) FILTER (WHERE true)", define))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:195: Cannot use FILTER with last pattern recognition function");

        assertFails(format(query, "LAST(Tradeday ORDER BY Tradeday)", define))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:195: Cannot use ORDER BY with last pattern recognition function");

        assertFails(format(query, "LAST(DISTINCT Tradeday)", define))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:195: Cannot use DISTINCT with last pattern recognition function");

        // test illegal clauses in DEFINE
        String measure = "true";
        assertFails(format(query, measure, "CLASSIFIER(Tradeday) OVER () > 0"))
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:313: Cannot nest window functions or row pattern measures inside pattern recognition expressions");

        assertFails(format(query, measure, "CLASSIFIER(Tradeday) FILTER (WHERE true) > 0"))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:313: Cannot use FILTER with classifier pattern recognition function");

        assertFails(format(query, measure, "CLASSIFIER(Tradeday ORDER BY Tradeday) > 0"))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:313: Cannot use ORDER BY with classifier pattern recognition function");

        assertFails(format(query, measure, "CLASSIFIER(DISTINCT Tradeday) > 0"))
                .hasErrorCode(INVALID_PATTERN_RECOGNITION_FUNCTION)
                .hasMessage("line 1:313: Cannot use DISTINCT with classifier pattern recognition function");

        // test quoted pattern recognition function name
        assertFails(format(query, "true", "\"PREV\"(Price)"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:313: Function 'prev' not registered");

        assertFails(format(query, "\"NEXT\"(Price) > 0", "true"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:195: Function 'next' not registered");

        assertFails(format(query, "true", "\"FIRST\"(Price)"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:313: Function 'first' not registered");

        assertFails(format(query, "\"LAST\"(Price) > 0", "true"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:195: Function 'last' not registered");

        assertFails(format(query, "true", "\"CLASSIFIER\"()"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:313: Function 'classifier' not registered");

        assertFails(format(query, "\"MATCH_NUMBER\"() > 0", "true"))
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:195: Function 'match_number' not registered");
    }

    @Test
    public void testRunningAndFinalSemantics()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS %s " +
                "                ) AS M";

        // pattern recognition functions in MEASURES
        String define = "true";
        analyze(format(query, "FINAL FIRST(Tradeday)", define));
        analyze(format(query, "FINAL LAST(Tradeday)", define));

        assertFails(format(query, "FINAL PREV(Tradeday)", define))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:195: FINAL semantics is not supported with prev pattern recognition function");

        assertFails(format(query, "FINAL NEXT(Tradeday)", define))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:195: FINAL semantics is not supported with next pattern recognition function");

        assertFails(format(query, "FINAL CLASSIFIER(Tradeday)", define))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:195: FINAL semantics is not supported with classifier pattern recognition function");

        assertFails(format(query, "FINAL MATCH_NUMBER(Tradeday)", define))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:195: FINAL semantics is not supported with match_number pattern recognition function");

        // scalar function in pattern recognition context
        assertFails(format(query, "FINAL lower(Tradeday)", define))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:195: FINAL semantics is supported only for FIRST(), LAST() and aggregation functions. Actual: lower");

        // out of pattern recognition context
        assertFails("SELECT FINAL avg(x) FROM (VALUES 1) t(x)")
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:8: FINAL semantics is not supported out of pattern recognition context");
    }

    @Test
    public void testPatternNavigationFunctions()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS true " +
                "                ) AS M";

        assertFails(format(query, "PREV()"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:195: prev pattern recognition function requires 1 or 2 arguments");

        assertFails(format(query, "PREV(Tradeday, 1, 'another')"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:195: prev pattern recognition function requires 1 or 2 arguments");

        assertFails(format(query, "PREV(Tradeday, 'text')"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:195: prev pattern recognition navigation function requires a number as the second argument");

        assertFails(format(query, "PREV(Tradeday, -5)"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:195: prev pattern recognition navigation function requires a non-negative number as the second argument (actual: -5)");

        assertFails(format(query, "PREV(Tradeday, 3000000000)"))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("line 1:195: The second argument of prev pattern recognition navigation function must not exceed 2147483647 (actual: 3000000000)");

        // nested navigations
        assertFails(format(query, "LAST(NEXT(Tradeday, 2))"))
                .hasErrorCode(INVALID_NAVIGATION_NESTING)
                .hasMessage("line 1:200: Cannot nest next pattern navigation function inside last pattern navigation function");

        assertFails(format(query, "PREV(NEXT(Tradeday, 2))"))
                .hasErrorCode(INVALID_NAVIGATION_NESTING)
                .hasMessage("line 1:200: Cannot nest next pattern navigation function inside prev pattern navigation function");

        analyze(format(query, "PREV(LAST(Tradeday, 2), 3)"));

        assertFails(format(query, "PREV(LAST(Tradeday, 2) + LAST(Tradeday, 3))"))
                .hasErrorCode(INVALID_NAVIGATION_NESTING)
                .hasMessage("line 1:220: Cannot nest multiple pattern navigation functions inside prev pattern navigation function");

        assertFails(format(query, "PREV(LAST(Tradeday, 2) + 5)"))
                .hasErrorCode(INVALID_NAVIGATION_NESTING)
                .hasMessage("line 1:200: Immediate nesting is required for pattern navigation functions");

        assertFails(format(query, "PREV(avg(Price) + 5)"))
                .hasErrorCode(NESTED_AGGREGATION)
                .hasMessage("line 1:200: Cannot nest avg aggregate function inside prev function");

        // navigation function must column reference or CLASSIFIER()
        assertFails(format(query, "PREV(LAST('no_column'))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: Pattern navigation function last must contain at least one column reference or CLASSIFIER()");

        analyze(format(query, "PREV(LAST(Tradeday + 1))"));
        analyze(format(query, "PREV(LAST(lower(CLASSIFIER())))"));

        // labels inside pattern navigation function (as column prefixes and CLASSIFIER arguments) must be consistent
        analyze(format(query, "PREV(LAST(length(CLASSIFIER(A)) + A.Tradeday + 1))"));
        analyze(format(query, "PREV(LAST(length(CLASSIFIER()) + Tradeday + 1))"));
        // mixed labels are allowed when not nested in navigation or aggregation
        analyze(format(query, "PREV(LAST(A.Tradeday)) + length(CLASSIFIER(B)) + Price + U.Price"));

        assertFails(format(query, "PREV(LAST(A.Tradeday + Price))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:205: Column references inside argument of function last must all either be prefixed with the same label or be not prefixed");

        assertFails(format(query, "PREV(LAST(A.Tradeday + B.Price))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:205: Column references inside argument of function last must all either be prefixed with the same label or be not prefixed");

        assertFails(format(query, "PREV(LAST(concat(CLASSIFIER(A), CLASSIFIER())))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: CLASSIFIER() calls inside argument of function last must all either have the same label as the argument or have no arguments");

        assertFails(format(query, "PREV(LAST(concat(CLASSIFIER(A), CLASSIFIER(B))))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: CLASSIFIER() calls inside argument of function last must all either have the same label as the argument or have no arguments");

        assertFails(format(query, "PREV(LAST(Tradeday + length(CLASSIFIER(B))))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: Column references inside argument of function last must all be prefixed with the same label that all CLASSIFIER() calls have as the argument");

        assertFails(format(query, "PREV(LAST(A.Tradeday + length(CLASSIFIER(B))))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: Column references inside argument of function last must all be prefixed with the same label that all CLASSIFIER() calls have as the argument");

        assertFails(format(query, "PREV(LAST(A.Tradeday + length(CLASSIFIER())))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:200: Column references inside argument of function last must all be prefixed with the same label that all CLASSIFIER() calls have as the argument");
    }

    @Test
    public void testClassifierFunction()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS true " +
                "                ) AS M";

        analyze(format(query, "CLASSIFIER(A)"));
        analyze(format(query, "CLASSIFIER(U)"));
        analyze(format(query, "CLASSIFIER()"));

        assertFails(format(query, "CLASSIFIER(A, B)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:195: CLASSIFIER pattern recognition function takes no arguments or 1 argument");

        assertFails(format(query, "CLASSIFIER(A.x)"))
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:206: CLASSIFIER function argument should be primary pattern variable or subset name. Actual: DereferenceExpression");

        assertFails(format(query, "CLASSIFIER(\"$\")"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:206: $ is not a primary pattern variable or subset name");

        assertFails(format(query, "CLASSIFIER(C)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:206: C is not a primary pattern variable or subset name");
    }

    @Test
    public void testMatchNumberFunction()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 9), (1, 2, 8)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   ORDER BY Tradeday " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS true " +
                "                ) AS M";

        analyze(format(query, "MATCH_NUMBER()"));

        assertFails(format(query, "MATCH_NUMBER(A)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:195: MATCH_NUMBER pattern recognition function takes no arguments");
    }

    @Test
    public void testPatternAggregations()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 1), (2, 2, 2)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS %s " +
                "                 ) AS M";

        // test illegal clauses in MEASURES
        String define = "true";
        assertFails(format(query, "max(Price) OVER ()", define))
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:158: Cannot nest window functions or row pattern measures inside pattern recognition expressions");

        assertFails(format(query, "max(Price) FILTER (WHERE true)", define))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:158: Cannot use FILTER with max aggregate function in pattern recognition context");

        assertFails(format(query, "max(Price ORDER BY Tradeday)", define))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:158: Cannot use ORDER BY with max aggregate function in pattern recognition context");

        assertFails(format(query, "LISTAGG(Price) WITHIN GROUP (ORDER BY Tradeday)", define))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:158: Cannot use ORDER BY with listagg aggregate function in pattern recognition context");

        assertFails(format(query, "max(DISTINCT Price)", define))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:158: Cannot use DISTINCT with max aggregate function in pattern recognition context");

        // test illegal clauses in DEFINE
        String measure = "true";
        assertFails(format(query, measure, "max(Price) OVER () > 0"))
                .hasErrorCode(NESTED_WINDOW)
                .hasMessage("line 1:276: Cannot nest window functions or row pattern measures inside pattern recognition expressions");

        assertFails(format(query, measure, "max(Price) FILTER (WHERE true) > 0"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:276: Cannot use FILTER with max aggregate function in pattern recognition context");

        assertFails(format(query, measure, "max(Price ORDER BY Tradeday) > 0"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:276: Cannot use ORDER BY with max aggregate function in pattern recognition context");

        assertFails(format(query, measure, "LISTAGG(Price) WITHIN GROUP (ORDER BY Tradeday) IS NOT NULL"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:276: Cannot use ORDER BY with listagg aggregate function in pattern recognition context");

        assertFails(format(query, measure, "max(DISTINCT Price) > 0"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:276: Cannot use DISTINCT with max aggregate function in pattern recognition context");
    }

    @Test
    public void testInvalidNestingInPatternAggregations()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 1), (2, 2, 2)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS true " +
                "                 ) AS M";

        assertFails(format(query, "max(1 + min(Price))"))
                .hasErrorCode(NESTED_AGGREGATION)
                .hasMessage("line 1:166: Cannot nest min aggregate function inside max function");
        assertFails(format(query, "max(1 + LAST(Price))"))
                .hasErrorCode(INVALID_NAVIGATION_NESTING)
                .hasMessage("line 1:166: Cannot nest last pattern navigation function inside max function");
    }

    @Test
    public void testLabelsInPatternAggregations()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 1), (2, 2, 2)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS true " +
                "                ) AS M";

        // at most one label inside argument
        analyze(format(query, "count()"));
        analyze(format(query, "count(Symbol)"));
        analyze(format(query, "count(A.Symbol)"));
        analyze(format(query, "count(U.Symbol)"));
        analyze(format(query, "count(CLASSIFIER())"));
        analyze(format(query, "count(CLASSIFIER(A))"));
        analyze(format(query, "count(CLASSIFIER(U))"));

        // consistent labels inside argument
        analyze(format(query, "count(Price < 5 OR CLASSIFIER() > 'X')"));
        analyze(format(query, "count(B.Price < 5 OR CLASSIFIER(B) > 'X')"));
        analyze(format(query, "count(U.Price < 5 OR CLASSIFIER(U) > 'X')"));

        // inconsistent labels inside argument
        assertFails(format(query, "count(B.Price < 5 OR Price > 5)"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:164: Column references inside argument of function count must all either be prefixed with the same label or be not prefixed");
        assertFails(format(query, "count(B.Price < 5 OR A.Price > 5)"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:164: Column references inside argument of function count must all either be prefixed with the same label or be not prefixed");
        assertFails(format(query, "count(CLASSIFIER(A) < 'X' OR CLASSIFIER(B) > 'Y')"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:158: CLASSIFIER() calls inside argument of function count must all either have the same label as the argument or have no arguments");
        assertFails(format(query, "count(Price < 5 OR CLASSIFIER(B) > 'Y')"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:158: Column references inside argument of function count must all be prefixed with the same label that all CLASSIFIER() calls have as the argument");
        assertFails(format(query, "count(A.Price < 5 OR CLASSIFIER(B) > 'Y')"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:158: Column references inside argument of function count must all be prefixed with the same label that all CLASSIFIER() calls have as the argument");

        // multiple aggregation arguments
        analyze(format(query, "max_by(Price, Symbol)"));
        analyze(format(query, "max_by(A.Price, A.Symbol)"));
        analyze(format(query, "max_by(U.Price, U.Symbol)"));

        analyze(format(query, "max_by(Price, 1)"));
        analyze(format(query, "max_by(A.Price, 1)"));
        analyze(format(query, "max_by(U.Price, 1)"));
        analyze(format(query, "max_by(1, 1)"));
        analyze(format(query, "max_by(1, Price)"));
        analyze(format(query, "max_by(1, A.Price)"));
        analyze(format(query, "max_by(1, U.Price)"));

        assertFails(format(query, "max_by(U.Price, A.Price)"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:158: All aggregate function arguments must apply to rows matched with the same label");

        // inconsistent labels in second argument
        assertFails(format(query, "max_by(A.Symbol, A.Price + B.price)"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:175: Column references inside argument of function max_by must all either be prefixed with the same label or be not prefixed");
    }

    @Test
    public void testRunningAndFinalPatternAggregations()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 1), (2, 2, 2)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE B AS %s " +
                "                ) AS M";

        // in MEASURES clause
        analyze(format(query, "RUNNING avg(A.Price)", "true"));
        analyze(format(query, "FINAL avg(A.Price)", "true"));

        // in DEFINE clause
        analyze(format(query, "true", "RUNNING avg(A.Price) > 5"));
        assertFails(format(query, "true", "FINAL avg(A.Price) > 5"))
                .hasErrorCode(INVALID_PROCESSING_MODE)
                .hasMessage("line 1:276: FINAL semantics is not supported in DEFINE clause");

        // count star aggregation
        analyze(format(query, "RUNNING count(*)", "count(*) >= 0"));
        analyze(format(query, "FINAL count(*)", "count(*) >= 0"));
        analyze(format(query, "RUNNING count()", "count() >= 0"));
        analyze(format(query, "FINAL count()", "count() >= 0"));
        analyze(format(query, "RUNNING count(A.*)", "count(B.*) >= 0"));
        analyze(format(query, "FINAL count(U.*)", "count(U.*) >= 0"));
    }

    @Test
    public void testRowPatternCountFunction()
    {
        String query = "SELECT M.Measure " +
                "          FROM (VALUES (1, 1, 1), (2, 2, 2)) Ticker(Symbol, Tradeday, Price) " +
                "                 MATCH_RECOGNIZE ( " +
                "                   MEASURES %s AS Measure " +
                "                   PATTERN (A B+) " +
                "                   SUBSET U = (A, B) " +
                "                   DEFINE A AS true " +
                "                ) AS M";

        analyze(format(query, "count(*)"));
        analyze(format(query, "count()"));
        analyze(format(query, "count(B.*)"));
        analyze(format(query, "count(U.*)"));

        assertFails("SELECT count(A.*) FROM (VALUES 1) t(a)")
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:14: label.* syntax is only supported as the only argument of row pattern count function");

        assertFails(format(query, "lower(A.*)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:164: label.* syntax is only supported as the only argument of row pattern count function");

        assertFails(format(query, "min(A.*)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:162: label.* syntax is only supported as the only argument of row pattern count function");

        assertFails(format(query, "count(X.*)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("line 1:164: X is not a primary pattern variable or subset name");
    }

    @Test
    public void testAnalyzeFreshMaterializedView()
    {
        analyze("SELECT * FROM fresh_materialized_view");
    }

    @Test
    public void testAnalyzeInvalidFreshMaterializedView()
    {
        assertFails("SELECT * FROM fresh_materialized_view_mismatched_column_count")
                .hasErrorCode(INVALID_VIEW)
                .hasMessage("line 1:15: storage table column count (2) does not match column count derived from the materialized view query analysis (1)");
        assertFails("SELECT * FROM fresh_materialized_view_mismatched_column_name")
                .hasErrorCode(INVALID_VIEW)
                .hasMessage("line 1:15: column [b] of type bigint projected from storage table at position 1 has a different name from column [c] of type bigint stored in materialized view definition");
        assertFails("SELECT * FROM fresh_materialized_view_mismatched_column_type")
                .hasErrorCode(INVALID_VIEW)
                .hasMessage("line 1:15: cannot cast column [b] of type bigint projected from storage table at position 1 into column [b] of type row(tinyint) stored in view definition");
    }

    @Test
    public void testAnalyzeMaterializedViewWithAccessControl()
    {
        TestingAccessControlManager accessControlManager = new TestingAccessControlManager(transactionManager, emptyEventListenerManager());
        accessControlManager.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));

        analyze("SELECT * FROM fresh_materialized_view");

        // materialized view analysis should succeed even if access to storage table is denied when querying the table directly
        accessControlManager.deny(privilege("t2.a", SELECT_COLUMN));
        analyze("SELECT * FROM fresh_materialized_view");

        accessControlManager.deny(privilege("fresh_materialized_view.a", SELECT_COLUMN));
        assertFails(
                CLIENT_SESSION,
                "SELECT * FROM fresh_materialized_view",
                accessControlManager)
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot select from columns [a, b] in table or view tpch.s1.fresh_materialized_view");
    }

    @BeforeClass
    public void setup()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION);
        closer.register(queryRunner);
        transactionManager = queryRunner.getTransactionManager();

        AccessControlManager accessControlManager = new AccessControlManager(
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                DefaultSystemAccessControl.NAME);
        accessControlManager.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        this.accessControl = accessControlManager;

        Metadata metadata = queryRunner.getMetadata();
        metadata.addFunctions(ImmutableList.of(APPLY_FUNCTION));
        plannerContext = plannerContextBuilder().withMetadata(metadata).build();

        TestingMetadata testingConnectorMetadata = new TestingMetadata();
        TestingConnector connector = new TestingConnector(testingConnectorMetadata);
        queryRunner.createCatalog(TPCH_CATALOG, new StaticConnectorFactory("main", connector), ImmutableMap.of());

        tablePropertyManager = queryRunner.getTablePropertyManager();
        analyzePropertyManager = queryRunner.getAnalyzePropertyManager();

        queryRunner.createCatalog(SECOND_CATALOG, MockConnectorFactory.create("second"), ImmutableMap.of());
        queryRunner.createCatalog(THIRD_CATALOG, MockConnectorFactory.create("third"), ImmutableMap.of());

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

        // materialized view referencing table in same schema
        MaterializedViewDefinition materializedViewData1 = new MaterializedViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("comment"),
                Identity.ofUser("user"),
                Optional.empty(),
                ImmutableMap.of());
        inSetupTransaction(session -> metadata.createMaterializedView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "mv1"), materializedViewData1, false, true));

        // valid view referencing table in same schema
        ViewDefinition viewData1 = new ViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("comment"),
                Optional.of(Identity.ofUser("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v1"), viewData1, false));

        // stale view (different column type)
        ViewDefinition viewData2 = new ViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", VARCHAR.getTypeId())),
                Optional.of("comment"),
                Optional.of(Identity.ofUser("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v2"), viewData2, false));

        // view referencing table in different schema from itself and session
        ViewDefinition viewData3 = new ViewDefinition(
                "select a from t4",
                Optional.of(SECOND_CATALOG),
                Optional.of("s2"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("comment"),
                Optional.of(Identity.ofUser("owner")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(THIRD_CATALOG, "s3", "v3"), viewData3, false));

        // valid view with uppercase column name
        ViewDefinition viewData4 = new ViewDefinition(
                "select A from t1",
                Optional.of("tpch"),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("comment"),
                Optional.of(Identity.ofUser("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName("tpch", "s1", "v4"), viewData4, false));

        // recursive view referencing to itself
        ViewDefinition viewData5 = new ViewDefinition(
                "select * from v5",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.of("comment"),
                Optional.of(Identity.ofUser("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v5"), viewData5, false));

        // type analysis for INSERT
        SchemaTableName table8 = new SchemaTableName("s1", "t8");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table8, ImmutableList.of(
                        new ColumnMetadata("tinyint_column", TINYINT),
                        new ColumnMetadata("integer_column", INTEGER),
                        new ColumnMetadata("decimal_column", createDecimalType(5, 3)),
                        new ColumnMetadata("real_column", REAL),
                        new ColumnMetadata("char_column", createCharType(3)),
                        new ColumnMetadata("bounded_varchar_column", createVarcharType(3)),
                        new ColumnMetadata("unbounded_varchar_column", VARCHAR),
                        new ColumnMetadata("tinyint_array_column", new ArrayType(TINYINT)),
                        new ColumnMetadata("bigint_array_column", new ArrayType(BIGINT)),
                        new ColumnMetadata("nested_bounded_varchar_column", anonymousRow(createVarcharType(3))),
                        new ColumnMetadata("row_column", anonymousRow(TINYINT, createUnboundedVarcharType())),
                        new ColumnMetadata("date_column", DATE))),
                false));

        // for identifier chain resolving tests
        queryRunner.createCatalog(CATALOG_FOR_IDENTIFIER_CHAIN_TESTS, new StaticConnectorFactory("chain", new TestingConnector(new TestingMetadata())), ImmutableMap.of());
        Type singleFieldRowType = TESTING_TYPE_MANAGER.fromSqlType("row(f1 bigint)");
        Type rowType = TESTING_TYPE_MANAGER.fromSqlType("row(f1 bigint, f2 bigint)");
        Type nestedRowType = TESTING_TYPE_MANAGER.fromSqlType("row(f1 row(f11 bigint, f12 bigint), f2 boolean)");
        Type doubleNestedRowType = TESTING_TYPE_MANAGER.fromSqlType("row(f1 row(f11 row(f111 bigint, f112 bigint), f12 boolean), f2 boolean)");

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

        QualifiedObjectName tableViewAndMaterializedView = new QualifiedObjectName(TPCH_CATALOG, "s1", "table_view_and_materialized_view");
        inSetupTransaction(session -> metadata.createMaterializedView(
                session,
                tableViewAndMaterializedView,
                new MaterializedViewDefinition(
                        "SELECT a FROM t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                        Optional.empty(),
                        Identity.ofUser("some user"),
                        Optional.of(new CatalogSchemaTableName(TPCH_CATALOG, "s1", "t1")),
                        ImmutableMap.of()),
                false,
                false));
        ViewDefinition viewDefinition = new ViewDefinition(
                "SELECT a FROM t2",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                Optional.empty(),
                Optional.empty());
        inSetupTransaction(session -> metadata.createView(
                session,
                tableViewAndMaterializedView,
                viewDefinition,
                false));
        inSetupTransaction(session -> metadata.createTable(
                session,
                CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(
                        tableViewAndMaterializedView.asSchemaTableName(),
                        ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                false));

        QualifiedObjectName tableAndView = new QualifiedObjectName(TPCH_CATALOG, "s1", "table_and_view");
        inSetupTransaction(session -> metadata.createView(
                session,
                tableAndView,
                viewDefinition,
                false));
        inSetupTransaction(session -> metadata.createTable(
                session,
                CATALOG_FOR_IDENTIFIER_CHAIN_TESTS,
                new ConnectorTableMetadata(
                        tableAndView.asSchemaTableName(),
                        ImmutableList.of(new ColumnMetadata("a", BIGINT))),
                false));

        QualifiedObjectName freshMaterializedView = new QualifiedObjectName(TPCH_CATALOG, "s1", "fresh_materialized_view");
        inSetupTransaction(session -> metadata.createMaterializedView(
                session,
                freshMaterializedView,
                new MaterializedViewDefinition(
                        "SELECT a, b FROM t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId()), new ViewColumn("b", BIGINT.getTypeId())),
                        Optional.empty(),
                        Identity.ofUser("some user"),
                        // t3 has a, b column and hidden column x
                        Optional.of(new CatalogSchemaTableName(TPCH_CATALOG, "s1", "t3")),
                        ImmutableMap.of()),
                false,
                false));
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedView.asSchemaTableName());

        QualifiedObjectName freshMaterializedViewMismatchedColumnCount = new QualifiedObjectName(TPCH_CATALOG, "s1", "fresh_materialized_view_mismatched_column_count");
        inSetupTransaction(session -> metadata.createMaterializedView(
                session,
                freshMaterializedViewMismatchedColumnCount,
                new MaterializedViewDefinition(
                        "SELECT a FROM t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId())),
                        Optional.empty(),
                        Identity.ofUser("some user"),
                        Optional.of(new CatalogSchemaTableName(TPCH_CATALOG, "s1", "t2")),
                        ImmutableMap.of()),
                false,
                false));
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedViewMismatchedColumnCount.asSchemaTableName());

        QualifiedObjectName freshMaterializedMismatchedColumnName = new QualifiedObjectName(TPCH_CATALOG, "s1", "fresh_materialized_view_mismatched_column_name");
        inSetupTransaction(session -> metadata.createMaterializedView(
                session,
                freshMaterializedMismatchedColumnName,
                new MaterializedViewDefinition(
                        "SELECT a, b as c FROM t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId()), new ViewColumn("c", BIGINT.getTypeId())),
                        Optional.empty(),
                        Identity.ofUser("some user"),
                        Optional.of(new CatalogSchemaTableName(TPCH_CATALOG, "s1", "t2")),
                        ImmutableMap.of()),
                false,
                false));
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedMismatchedColumnName.asSchemaTableName());

        QualifiedObjectName freshMaterializedMismatchedColumnType = new QualifiedObjectName(TPCH_CATALOG, "s1", "fresh_materialized_view_mismatched_column_type");
        inSetupTransaction(session -> metadata.createMaterializedView(
                session,
                freshMaterializedMismatchedColumnType,
                new MaterializedViewDefinition(
                        "SELECT a, null b FROM t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewColumn("a", BIGINT.getTypeId()), new ViewColumn("b", RowType.anonymousRow(TINYINT).getTypeId())),
                        Optional.empty(),
                        Identity.ofUser("some user"),
                        Optional.of(new CatalogSchemaTableName(TPCH_CATALOG, "s1", "t2")),
                        ImmutableMap.of()),
                false,
                false));
        testingConnectorMetadata.markMaterializedViewIsFresh(freshMaterializedMismatchedColumnType.asSchemaTableName());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private Analyzer createAnalyzer(Session session, AccessControl accessControl)
    {
        StatementRewrite statementRewrite = new StatementRewrite(ImmutableSet.of(new ShowQueriesRewrite(
                plannerContext.getMetadata(),
                SQL_PARSER,
                accessControl,
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new ColumnPropertyManager(),
                tablePropertyManager,
                new MaterializedViewPropertyManager())));
        StatementAnalyzerFactory statementAnalyzerFactory = createTestingStatementAnalyzerFactory(plannerContext, accessControl, tablePropertyManager, analyzePropertyManager);
        AnalyzerFactory analyzerFactory = new AnalyzerFactory(statementAnalyzerFactory, statementRewrite);
        return analyzerFactory.createAnalyzer(
                session,
                emptyList(),
                emptyMap(),
                WarningCollector.NOOP);
    }

    private Analysis analyze(@Language("SQL") String query)
    {
        return analyze(CLIENT_SESSION, query);
    }

    private Analysis analyze(Session clientSession, @Language("SQL") String query)
    {
        return analyze(clientSession, query, new AllowAllAccessControl());
    }

    private Analysis analyze(Session clientSession, @Language("SQL") String query, AccessControl accessControl)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, accessControl);
                    Statement statement = SQL_PARSER.createStatement(query, new ParsingOptions(
                            new FeaturesConfig().isParseDecimalLiteralsAsDouble() ? AS_DOUBLE : AS_DECIMAL));
                    return analyzer.analyze(statement);
                });
    }

    private TrinoExceptionAssert assertFails(@Language("SQL") String query)
    {
        return assertFails(CLIENT_SESSION, query);
    }

    private TrinoExceptionAssert assertFails(Session session, @Language("SQL") String query)
    {
        return assertFails(session, query, new AllowAllAccessControl());
    }

    private TrinoExceptionAssert assertFails(Session session, @Language("SQL") String query, AccessControl accessControl)
    {
        return assertTrinoExceptionThrownBy(() -> analyze(session, query, accessControl));
    }

    private static class TestingConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        public TestingConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return new ConnectorTransactionHandle() {};
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
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
    }
}
