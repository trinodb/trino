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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.ColumnNaming;
import io.trino.security.AccessControl;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.ColumnNaming.SIMPLIFIED;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestIcebergBucketedTables
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestIcebergBucketedTables.class);
    private static final String BUCKETED_SCHEMA = "tpch_bucketed";
    private static final List<TpchTable<?>> BUCKETED_TPCH_TABLES = ImmutableList.of(CUSTOMER, ORDERS);

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .build();
    }

    @Override
    protected Session getSession()
    {
        return getSession(super.getSession().getSchema());
    }

    protected Session getBucketedSession()
    {
        return getSession(Optional.of(BUCKETED_SCHEMA));
    }

    protected Session getSession(Optional<String> schema)
    {
        Session defaultSession = super.getSession();
        return Session.builder(defaultSession)
                .setSchema(schema)
                .setSystemProperty("colocated_join", "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, "bucket_execution_enabled", "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, "allow_multi_spec_bucket_execution", "true")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        assertUpdate(format("CREATE SCHEMA %s", BUCKETED_SCHEMA));
        copyTpchTablesBucketed(getDistributedQueryRunner(), "tpch", TINY_SCHEMA_NAME, getBucketedSession(), BUCKETED_TPCH_TABLES, SIMPLIFIED);
        assertUpdate("CREATE TABLE bucketed_table1 (_bigint BIGINT, _date DATE, _varchar VARCHAR, _data1 BIGINT) " +
                "WITH (partitioning = ARRAY['_date', 'bucket(_bigint, 2)'])");
        assertUpdate("INSERT INTO bucketed_table1 VALUES " +
                        "(0, CAST('2019-09-08' AS DATE), 'a', 10), " +
                        "(1, CAST('2019-09-08' AS DATE), 'b', 11)," +
                        "(2, CAST('2019-09-08' AS DATE), 'c', 12)",
                3);
        assertUpdate("ALTER TABLE bucketed_table1 SET PROPERTIES " +
                "partitioning = ARRAY['_date', 'bucket(_varchar, 2)']");
        assertUpdate("INSERT INTO bucketed_table1 VALUES " +
                        "(0, CAST('2019-09-09' AS DATE), 'a', 10), " +
                        "(1, CAST('2019-09-09' AS DATE), 'b', 11)," +
                        "(2, CAST('2019-09-09' AS DATE), 'c', 12)",
                3);
        assertUpdate("ALTER TABLE bucketed_table1 SET PROPERTIES " +
                "partitioning = ARRAY['_date', 'bucket(_bigint, 4)', 'bucket(_varchar, 4)']");
        assertUpdate("INSERT INTO bucketed_table1 VALUES " +
                        "(0, CAST('2019-09-10' AS DATE), 'a', 10), " +
                        "(1, CAST('2019-09-10' AS DATE), 'b', 11)," +
                        "(2, CAST('2019-09-10' AS DATE), 'c', 12)",
                3);
        // to make two table join-able via bucketed join, the order of definition of joining bucketed partition column must be same in partitioning specs
        // they also have to have matching bucket numbers
        assertUpdate("CREATE TABLE bucketed_table2 (_bigint BIGINT, _date DATE, _varchar VARCHAR, _data2 BIGINT) " +
                "WITH (partitioning = ARRAY['_date', 'bucket(_bigint, 4)', 'bucket(_varchar, 4)'])");
        assertUpdate("INSERT INTO bucketed_table2 VALUES " +
                        "(0, CAST('2019-09-08' AS DATE), 'a', 200), " +
                        "(1, CAST('2019-09-08' AS DATE), 'b', 201)," +
                        "(2, CAST('2019-09-08' AS DATE), 'c', 202)," +
                        "(0, CAST('2019-09-09' AS DATE), 'a', 210), " +
                        "(1, CAST('2019-09-09' AS DATE), 'b', 211)," +
                        "(2, CAST('2019-09-09' AS DATE), 'c', 212), " +
                        "(0, CAST('2019-09-10' AS DATE), 'a', 220), " +
                        "(1, CAST('2019-09-10' AS DATE), 'b', 221)," +
                        "(2, CAST('2019-09-10' AS DATE), 'c', 222)",
                9);
    }

    @AfterAll
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS bucketed_table1");
        assertUpdate("DROP TABLE IF EXISTS bucketed_table2");
        for (TpchTable<?> table : BUCKETED_TPCH_TABLES) {
            assertUpdate(format("DROP TABLE IF EXISTS %s.%s", BUCKETED_SCHEMA, table.getTableName()));
        }
        assertUpdate(format("DROP SCHEMA IF EXISTS %s", BUCKETED_SCHEMA));
    }

    @Test
    public void testJoinTwoTablesBucketed()
    {
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();
        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        AccessControl accessControl = getQueryRunner().getAccessControl();

        // 09-08 both are bucketed by _bigint, so should be OK if join key is only _bigint.
        // if we join by both _bigint and _varchar, table1 is not partitioned by _varchar while table2
        // are, we cannot do co-located join.
        // the above is technically achievable but need engine change to pass down symbol mapping.
        transaction(transactionManager, metadata, accessControl)
                .execute(
                        getSession(),
                        transactionedSession -> {
                            assertQuery(
                                    "SELECT _data1, _data2 FROM bucketed_table1 a "
                                            + "JOIN bucketed_table2 b "
                                            + "ON a._bigint = b._bigint "
                                            + "AND a._date = b._date "
                                            + "WHERE a._date = CAST('2019-09-08' AS DATE)",
                                    "VALUES " + "(10, 200)," + "(11, 201)," + "(12, 202)",
                                    // We should expect no remote exchange to shuffle data across machines
                                    // but we should expect local exchange to shuffle data due to different bucket
                                    // numbers.
                                    allOK(
                                            assertRemoteExchangesCount(transactionedSession, 0),
                                            assertLocalExchangesCount(
                                                    transactionedSession, 1, ImmutableSet.of(ExchangeNode.Type.GATHER))));
                        });

        // 09-09 both are bucketed by _varchar, so should be OK be joined by _varchar but not joined by
        // _date/_var
        // 09-10 both are bucketed by _varchar/_date, so should be OK be joined by _date/_varchar

        transaction(transactionManager, metadata, accessControl)
                .execute(
                        getSession(),
                        transactionedSession -> {
                            assertQuery(
                                    "SELECT _data1, _data2 FROM bucketed_table1 a "
                                            + "JOIN bucketed_table2 b "
                                            + "ON a._bigint = b._bigint "
                                            + "AND a._varchar = b._varchar "
                                            + "AND a._date = b._date "
                                            + "WHERE a._date = CAST('2019-09-10' AS DATE)",
                                    "VALUES " + "(10, 220)," + "(11, 221)," + "(12, 222)",
                                    allOK(
                                            assertRemoteExchangesCount(transactionedSession, 0),
                                            assertLocalExchangesCount(
                                                    transactionedSession, 1, ImmutableSet.of(ExchangeNode.Type.GATHER))));
                        });

        // Following we won't do bucketed join because table1 has only 1 bucketed column projected,
        // table2 has 2
        transaction(transactionManager, metadata, accessControl)
                .execute(
                        getSession(),
                        transactionedSession -> {
                            assertQuery(
                                    "SELECT _data1, _data2, b._varchar FROM bucketed_table1 a "
                                            + "JOIN bucketed_table2 b "
                                            + "ON a._bigint = b._bigint "
                                            + "AND a._date = b._date "
                                            + "WHERE a._date = CAST('2019-09-10' AS DATE)",
                                    "VALUES " + "(10, 220, 'a')," + "(11, 221, 'b')," + "(12, 222, 'c')",
                                    allOK(
                                            assertRemoteExchangesCount(transactionedSession, 1),
                                            assertLocalExchangesCount(
                                                    transactionedSession, 1, ImmutableSet.of(ExchangeNode.Type.GATHER))));
                        });
    }

    @Test
    public void testBucketedExecution()
    {
        assertQuery(getBucketedSession(), "SELECT count(*) a FROM orders t1 JOIN orders t2 on t1.custkey=t2.custkey");
        assertQuery(getBucketedSession(), "SELECT count(*) a FROM orders t1 JOIN customer t2 on t1.custkey=t2.custkey", "SELECT count(*) FROM orders");
        assertQuery(getBucketedSession(), "SELECT count(distinct custkey) FROM orders");
        assertQuery(getBucketedSession(), "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    private Consumer<Plan> allOK(Consumer<Plan>... assertions)
    {
        return plan -> {
            for (int i = 0; i < assertions.length; i++) {
                assertions[i].accept(plan);
            }
        };
    }

    private Consumer<Plan> assertRemoteExchangesCount(Session session, int expectedRemoteExchangesCount)
    {
        return plan -> {
            int actualRemoteExchangesCount =
                    searchFrom(plan.getRoot())
                            .where(
                                    node ->
                                            node instanceof ExchangeNode
                                                    && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE)
                            .findAll()
                            .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();
                FunctionManager functionManager =
                        getDistributedQueryRunner().getPlannerContext().getFunctionManager();
                String formattedPlan =
                        textLogicalPlan(
                                plan.getRoot(),
                                metadata,
                                functionManager,
                                StatsAndCosts.empty(),
                                session,
                                0,
                                false);
                throw new AssertionError(
                        format(
                                "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                                expectedRemoteExchangesCount, actualRemoteExchangesCount, formattedPlan));
            }
        };
    }

    private Consumer<Plan> assertLocalExchangesCount(Session session, int expectedLocalExchangesCount, Set<ExchangeNode.Type> types)
    {
        return plan -> {
            int actualLocalExchangesCount =
                    searchFrom(plan.getRoot())
                            .where(
                                    node -> {
                                        if (!(node instanceof ExchangeNode exchangeNode)) {
                                            return false;
                                        }

                                        return exchangeNode.getScope() == ExchangeNode.Scope.LOCAL
                                                && types.contains(exchangeNode.getType());
                                    })
                            .findAll()
                            .size();
            if (actualLocalExchangesCount != expectedLocalExchangesCount) {
                Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();
                FunctionManager functionManager =
                        getDistributedQueryRunner().getPlannerContext().getFunctionManager();
                String formattedPlan =
                        textLogicalPlan(
                                plan.getRoot(),
                                metadata,
                                functionManager,
                                StatsAndCosts.empty(),
                                session,
                                0,
                                false);
                throw new AssertionError(
                        format(
                                "Expected [\n%s\n] local repartitioned exchanges but found [\n%s\n] local repartitioned exchanges. Actual plan is [\n\n%s\n]",
                                expectedLocalExchangesCount, actualLocalExchangesCount, formattedPlan));
            }
        };
    }

    private static void copyTpchTablesBucketed(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables,
            ColumnNaming columnNaming)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableBucketed(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH)), table, session, columnNaming);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableBucketed(QueryRunner queryRunner, QualifiedObjectName tableName, TpchTable<?> table, Session session, ColumnNaming columnNaming)
    {
        long start = System.nanoTime();
        @Language("SQL") String sql;
        switch (tableName.objectName()) {
            case "part":
            case "partsupp":
            case "supplier":
            case "nation":
            case "region":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", tableName.objectName(), tableName);
                break;
            case "lineitem":
                sql = format(
                        "CREATE TABLE %s WITH (partitioning = ARRAY['bucket(%s, 8)']) AS SELECT * FROM %s",
                        tableName.objectName(),
                        columnNaming.getName(table.getColumn("orderkey")),
                        tableName);
                break;
            case "customer":
            case "orders":
                sql = format(
                        "CREATE TABLE %s WITH (partitioning = ARRAY['bucket(%s, 8)']) AS SELECT * FROM %s",
                        tableName.objectName(),
                        columnNaming.getName(table.getColumn("custkey")),
                        tableName);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, tableName.objectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }
}
