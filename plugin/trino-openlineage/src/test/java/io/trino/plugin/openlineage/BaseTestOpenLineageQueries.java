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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.SessionRepresentation;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.net.URI;
import java.util.Locale;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
* - TestOpenLineageEventsFromQueries requires sequential execution to properly reset memory listener
* - TestOpenLineageEventListenerMarquezIntegration multiple parallel tests are flaky, as they increase wait time for marquez to register events
*/
@Execution(ExecutionMode.SAME_THREAD)
public abstract class BaseTestOpenLineageQueries
        extends AbstractTestQueryFramework
{
    private static final Duration TIMEOUT = Duration.valueOf("10s");
    protected static final String TRINO_URI = "http://trino-integration-test:1337";
    protected static final String OPEN_LINEAGE_NAMESPACE = format("trino://%s:%s", URI.create(TRINO_URI).getHost(), URI.create(TRINO_URI).getPort());

    @Test
    void testCreateTableAsSelectFromTable()
            throws Exception
    {
        for (LineageTestTableType tableType : LineageTestTableType.values()) {
            String outputTable = switch (tableType) {
                case TABLE, VIEW -> format("marquez.default.test_create_%s_as_select_from_table", tableType.toNameSuffix());
                case MATERIALIZED_VIEW -> format("mock.mock.test_create_%s_as_select_from_table", tableType.toNameSuffix());
            };

            @Language("SQL") String createTableQuery = format(
                    "CREATE %s %s AS SELECT * FROM tpch.tiny.nation",
                    tableType.queryReplacement(),
                    outputTable);

            String queryId = this.getQueryRunner()
                    .executeWithPlan(this.getSession(), createTableQuery)
                    .queryId()
                    .toString();

            assertEventually(TIMEOUT, () -> assertCreateTableAsSelectFromTable(
                    queryId,
                    createTableQuery,
                    outputTable,
                    tableType,
                    this.getSession().toSessionRepresentation()));
        }
    }

    public abstract void assertCreateTableAsSelectFromTable(
            String queryId,
            String query,
            String fullTableName,
            LineageTestTableType tableType,
            SessionRepresentation session)
            throws Exception;

    @Test
    void testCreateTableAsSelectFromView()
            throws Exception
    {
        for (LineageTestTableType tableType : LineageTestTableType.values()) {
            String viewName = format("test_view_%s", tableType.toNameSuffix());
            String outputTable = switch (tableType) {
                case TABLE, VIEW -> format("marquez.default.test_create_%s_as_select_from_view", tableType.toNameSuffix());
                case MATERIALIZED_VIEW -> format("mock.mock.test_create_%s_as_select_from_view", tableType.toNameSuffix());
            };

            @Language("SQL") String createViewQuery = format(
                    "CREATE VIEW %s AS SELECT * FROM tpch.tiny.nation",
                    viewName);

            String createViewQueryId = this.getQueryRunner()
                    .executeWithPlan(this.getSession(), createViewQuery)
                    .queryId()
                    .toString();

            @Language("SQL") String createTableQuery = format(
                    "CREATE %s %s AS SELECT * FROM %s",
                    tableType.queryReplacement(),
                    outputTable,
                    viewName);

            String createTableQueryId = this.getQueryRunner()
                    .executeWithPlan(this.getSession(), createTableQuery)
                    .queryId()
                    .toString();

            assertEventually(TIMEOUT, () -> assertCreateTableAsSelectFromView(
                    createViewQueryId,
                    createViewQuery,
                    createTableQueryId,
                    createTableQuery,
                    viewName,
                    outputTable,
                    tableType,
                    this.getSession().toSessionRepresentation()));
        }
    }

    public abstract void assertCreateTableAsSelectFromView(
            String createViewQueryId,
            String createViewQuery,
            String createTableQueryId,
            String createTableQuery,
            String viewName,
            String fullTableName,
            LineageTestTableType tableType,
            SessionRepresentation session)
            throws Exception;

    @Test
    void testCreateTableWithJoin()
            throws Exception
    {
        String outputTable = "test_create_table_with_join";

        @Language("SQL") String createTableWithJoinQuery = format("""
                CREATE TABLE %s AS
                SELECT
                    n.name AS nation,
                    COUNT(*) AS order_count,
                    SUM(o.totalprice) AS total_revenue,
                    AVG(o.totalprice) AS avg_order_value
                FROM tpch.tiny.nation n
                JOIN tpch.sf1.customer c ON n.nationkey = c.nationkey
                JOIN tpch.tiny.orders o ON c.custkey = o.custkey
                WHERE o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                GROUP BY n.name
                ORDER BY total_revenue DESC""", outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableWithJoinQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertCreateTableWithJoin(createTableQueryId, createTableWithJoinQuery, this.getSession().toSessionRepresentation()));
    }

    abstract void assertCreateTableWithJoin(String createTableQueryId, String createTableQuery, SessionRepresentation session)
            throws Exception;

    @Test
    void testCreateTableWithCTE()
            throws Exception
    {
        String outputTable = "monthly_store_rankings";

        @Language("SQL") String createTableWithCTEQuery = format("""
                CREATE TABLE %s AS
                WITH
                  monthly_sales AS (
                    SELECT
                      d.d_year,
                      d.d_moy,
                      s.s_store_sk,
                      s.s_store_name,
                      SUM(ss.ss_sales_price) AS monthly_total
                    FROM
                      tpcds.tiny.store_sales ss
                      JOIN tpcds.tiny.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
                      JOIN tpcds.tiny.store s ON ss.ss_store_sk = s.s_store_sk
                    WHERE
                      d.d_year = 2001
                    GROUP BY
                      d.d_year,
                      d.d_moy,
                      s.s_store_sk,
                      s.s_store_name
                  ),
                  store_rankings AS (
                    SELECT
                      d_year,
                      d_moy,
                      s_store_sk,
                      s_store_name,
                      monthly_total,
                      RANK() OVER (
                        PARTITION BY
                          d_year,
                          d_moy
                        ORDER BY
                          monthly_total DESC
                      ) as store_rank
                    FROM
                      monthly_sales
                  )
                SELECT
                  d_year,
                  d_moy,
                  CAST(d_year AS VARCHAR) || '-' || LPAD(CAST(d_moy AS VARCHAR), 2, '0') AS year_month,
                  s_store_name,
                  monthly_total,
                  store_rank
                FROM
                  store_rankings
                WHERE
                  store_rank <= 5
                ORDER BY
                  d_year,
                  d_moy,
                  store_rank""", outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableWithCTEQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertCreateTableWithCTE(createTableQueryId, createTableWithCTEQuery, this.getSession().toSessionRepresentation()));
    }

    abstract void assertCreateTableWithCTE(String createTableQueryId, String createTableQuery, SessionRepresentation session)
            throws Exception;

    @Test
    void testCreateTableWithSubquery()
            throws Exception
    {
        String outputTable = "active_suppliers";

        @Language("SQL") String createTableWithSubqueryQuery = format("""
                CREATE TABLE %s AS
                SELECT
                  s.suppkey,
                  s.name,
                  s.address,
                  s.phone,
                  n.name as nation_name
                FROM
                  tpch.tiny.supplier s
                  JOIN tpch.tiny.nation n ON s.nationkey = n.nationkey
                WHERE
                  EXISTS (
                    SELECT
                      1
                    FROM
                      tpch.tiny.lineitem l
                      JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey
                    WHERE
                      l.suppkey = s.suppkey
                      AND o.orderdate >= DATE '1996-01-01'
                      AND l.quantity > 30
                  )""", outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableWithSubqueryQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertCreateTableWithSubquery(createTableQueryId, createTableWithSubqueryQuery, this.getSession().toSessionRepresentation()));
    }

    abstract void assertCreateTableWithSubquery(String createTableQueryId, String createTableQuery, SessionRepresentation session)
            throws Exception;

    @Test
    void testCreateTableWithSetOperation()
            throws Exception
    {
        for (String setOperator : ImmutableList.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
            String outputTable = format("marquez.default.cross_dataset_analysis_%s", setOperator.toLowerCase(Locale.ENGLISH).replace(" ", "_"));

            @Language("SQL") String createTableWithUnionQuery = format("""
                    CREATE TABLE %s AS
                    SELECT
                      'TPC-H' AS dataset,
                      'Customer Orders' AS metric_type,
                      COUNT(*) AS record_count,
                      SUM(totalprice) AS total_value,
                      AVG(totalprice) AS avg_value
                    FROM
                      tpch.tiny.orders
                    WHERE
                      orderdate >= DATE '1995-01-01'
                    %s
                    SELECT
                      'TPC-DS' AS dataset,
                      'Store Sales' AS metric_type,
                      COUNT(*) AS record_count,
                      SUM(ss_sales_price) AS total_value,
                      AVG(ss_sales_price) AS avg_value
                    FROM
                      tpcds.tiny.store_sales ss
                      JOIN tpcds.tiny.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
                    WHERE
                      d.d_year >= 1998""", outputTable, setOperator);

            String createTableQueryId = this.getQueryRunner()
                    .executeWithPlan(this.getSession(), createTableWithUnionQuery)
                    .queryId()
                    .toString();

            assertEventually(TIMEOUT, () -> assertCreateTableWithUnion(createTableQueryId, createTableWithUnionQuery, outputTable, this.getSession().toSessionRepresentation()));
        }
    }

    abstract void assertCreateTableWithUnion(String createTableQueryId, String createTableQuery, String fullTableName, SessionRepresentation session)
            throws Exception;

    @Test
    public void testInsertQuery()
            throws Exception
    {
        String outputTable = "marquez.default.test_insert_into_create_as_select_from_table";

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation",
                outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        @Language("SQL") String insertIntoTableQuery = format(
                "INSERT INTO %s SELECT * FROM tpch.tiny.nation",
                outputTable);

        String insertIntoTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), insertIntoTableQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertInsertIntoTable(
                createTableQueryId,
                createTableQuery,
                insertIntoTableQueryId,
                insertIntoTableQuery,
                outputTable,
                this.getSession().toSessionRepresentation()));
    }

    abstract void assertInsertIntoTable(
            String createTableQueryId,
            String createTableQuery,
            String insertQueryId,
            String insertQuery,
            String fullTableName,
            SessionRepresentation session)
            throws Exception;

    @Test
    public void testDeleteQuery()
            throws Exception
    {
        String outputTable = "blackhole.schema_delete.customer_backup";

        @Language("SQL")
        String createSchemaQuery = "CREATE SCHEMA blackhole.schema_delete";

        String createSchemaQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createSchemaQuery)
                .queryId()
                .toString();

        @Language("SQL") String createTableQuery = format("""
                CREATE TABLE %s AS
                 SELECT
                     custkey,
                     name,
                     mktsegment,
                     nationkey
                 FROM tpch.tiny.customer
                """, outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        @Language("SQL") String deleteQuery = format("""
                DELETE FROM %s
                WHERE custkey IN (
                    SELECT c.custkey
                    FROM tpch.tiny.customer c
                    WHERE c.acctbal < 5000.0
                )
                """, outputTable);

        String deleteQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), deleteQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertDeleteFromTable(
                createSchemaQueryId,
                createSchemaQuery,
                createTableQueryId,
                createTableQuery,
                deleteQueryId,
                deleteQuery,
                outputTable,
                this.getSession().toSessionRepresentation()));
    }

    abstract void assertDeleteFromTable(
            String createSchemaQueryId,
            String createSchemaQuery,
            String createTableQueryId,
            String createTableQuery,
            String deleteQueryId,
            String deleteQuery,
            String fullTableName,
            SessionRepresentation session)
            throws Exception;

    @Test
    public void testMergeIntoTableQuery()
            throws Exception
    {
        String outputTable = "blackhole.schema_merge.customer_backup";

        @Language("SQL")
        String createSchemaQuery = "CREATE SCHEMA blackhole.schema_merge";

        String createSchemaQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createSchemaQuery)
                .queryId()
                .toString();

        @Language("SQL") String createTableQuery = format("""
                CREATE TABLE %s AS
                 SELECT
                     custkey,
                     name,
                     mktsegment,
                     nationkey
                 FROM tpch.tiny.customer
                """, outputTable);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        @Language("SQL") String mergeQuery = format("""
                MERGE INTO %s cb
                 USING (
                     SELECT custkey, name, mktsegment, nationkey
                     FROM tpch.tiny.customer
                     WHERE mktsegment = 'BUILDING'
                 ) AS building_customers ON cb.custkey = building_customers.custkey
                 WHEN MATCHED THEN
                     UPDATE SET name = building_customers.name
                 WHEN NOT MATCHED THEN
                     INSERT (custkey, name, mktsegment, nationkey)
                     VALUES (building_customers.custkey, building_customers.name, building_customers.mktsegment, building_customers.nationkey)
                """, outputTable);

        String mergeQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), mergeQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertMergeIntoTable(
                createSchemaQueryId,
                createSchemaQuery,
                createTableQueryId,
                createTableQuery,
                mergeQueryId,
                mergeQuery,
                outputTable,
                this.getSession().toSessionRepresentation()));
    }

    abstract void assertMergeIntoTable(
            String createSchemaQueryId,
            String createSchemaQuery,
            String createTableQueryId,
            String createTableQuery,
            String mergeQueryId,
            String mergeQuery,
            String fullTableName,
            SessionRepresentation session)
            throws Exception;

    public enum LineageTestTableType
    {
        TABLE(
                "TABLE",
                QueryType.INSERT,
                true,
                46,
                true),
        VIEW(
                "VIEW",
                QueryType.DATA_DEFINITION,
                false,
                45,
                false),
        MATERIALIZED_VIEW(
                "MATERIALIZED VIEW",
                QueryType.DATA_DEFINITION,
                false,
                45,
                false);

        private final String queryReplacement;
        private final QueryType ctasQueryType;
        private final boolean hasQueryPlanInEvent;
        private final int numberOfStatistics;
        private final boolean hasColumnLineageDataset;

        LineageTestTableType(
                String queryReplacement,
                QueryType ctasQueryType,
                boolean hasQueryPlanInEvent,
                int numberOfStatistics,
                boolean hasColumnLineageDataset)
        {
            this.queryReplacement = requireNonNull(queryReplacement, "queryReplacement is null");
            this.ctasQueryType = requireNonNull(ctasQueryType, "ctasQueryType is null");
            this.hasQueryPlanInEvent = hasQueryPlanInEvent;
            this.numberOfStatistics = numberOfStatistics;
            this.hasColumnLineageDataset = hasColumnLineageDataset;
        }

        String toNameSuffix()
        {
            return queryReplacement.toLowerCase(Locale.ENGLISH).replace(" ", "_");
        }

        String queryReplacement()
        {
            return queryReplacement;
        }

        QueryType ctasQueryType()
        {
            return ctasQueryType;
        }

        boolean hasQueryPlanInEvent()
        {
            return hasQueryPlanInEvent;
        }

        int numberOfStatistics()
        {
            return numberOfStatistics;
        }

        boolean hasColumnLineageDataset()
        {
            return hasColumnLineageDataset;
        }
    }
}
