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
package io.trino.plugin.snowflake;

import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.operator.OperatorStats;
import io.trino.server.DynamicFilterService;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.MaterializedRow;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.jdbc.JdbcJoinPushdownSessionProperties.JOIN_PUSHDOWN_STRATEGY;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class AbstractTestBase
{
    private DistributedQueryRunner queryRunner;

    public static Session sessionOf(String user)
    {
        Session session = SnowflakeSqlQueryRunner.createSession();
        return Session.builder(session)
                .setIdentity(Identity.ofUser(user)).build();
    }

    public static void copyTables(DistributedQueryRunner queryRunner)
            throws SQLException
    {
        Optional<String> catalog = SnowflakeSqlQueryRunner.getCatalog();
        String schema = "tpch";
        MaterializedResult schemas = queryRunner.execute("SHOW SCHEMAS FROM snowflake");
        List<TpchTable<?>> tpchTables = new ArrayList<>(getTables());
        List<String> tpchTablesStr = getTables().stream().map(TpchTable::getTableName).collect(Collectors.toList());
        Optional<MaterializedRow> hasSchema = schemas.getMaterializedRows().stream()
                .filter(f -> f.getField(0).toString().equalsIgnoreCase(schema))
                .findFirst();
        if (hasSchema.isPresent()) {
            MaterializedResult tables = queryRunner.execute("SHOW TABLES FROM snowflake." + schema);
            tables.getMaterializedRows().stream()
                    .map(f -> f.getField(0).toString())
                    .forEach(f -> {
                        if (tpchTablesStr.contains(f)) {
                            int index = tpchTablesStr.indexOf(f);
                            tpchTables.remove(index);
                            tpchTablesStr.remove(index);
                        }
                    });
        }
        else {
            Connection connection = SnowflakeSqlQueryRunner.getConnection();
            Statement statement = connection.createStatement();
            statement.executeQuery("DROP SCHEMA IF EXISTS " + catalog.get() + "." + schema);
            statement.executeQuery("CREATE SCHEMA " + catalog.get() + "." + schema);
        }
        copyTpchTables(queryRunner, schema, "tiny",
                queryRunner.getDefaultSession(), tpchTables);
    }

    @BeforeTest
    public void setup()
            throws Exception
    {
        SnowflakeSqlQueryRunner.setup();
        queryRunner = getQueryRunner();
    }

    @AfterClass
    @AfterTest
    public void close()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    DistributedQueryRunner getQueryRunner()
            throws Exception
    {
        if (queryRunner == null) {
            return SnowflakeSqlQueryRunner
                    .createSnowflakeSqlQueryRunner(SnowflakeSqlQueryRunner.getCatalog(),
                            SnowflakeSqlQueryRunner.getWarehouse());
        }
        else {
            return queryRunner;
        }
    }

    void assertPlanPushdown(String query, long expectedInputRows, long expectedOutputRows)
    {
        Session defaultSession = queryRunner.getDefaultSession();
        Session session =
                Session.builder(defaultSession)
                        .setSystemProperty("enable_dynamic_filtering", "true")
                        .setCatalogSessionProperty(defaultSession.getCatalog().orElseThrow(), JOIN_PUSHDOWN_ENABLED, "true")
                        .setCatalogSessionProperty(defaultSession.getCatalog().orElseThrow(), JOIN_PUSHDOWN_STRATEGY, "EAGER")
                        .build();
        MaterializedResultWithQueryId result = queryRunner.executeWithQueryId(
                session, query);
        QueryInfo fullQueryInfo = queryRunner.getCoordinator().getQueryManager()
                .getFullQueryInfo(result.getQueryId());
        List<DynamicFilterService.DynamicFilterDomainStats> dynamicFilterDomainStats = fullQueryInfo.getQueryStats().getDynamicFiltersStats().getDynamicFilterDomainStats();
        System.out.println(dynamicFilterDomainStats);
        List<OperatorStats> operatorSummaries = fullQueryInfo.getQueryStats()
                .getOperatorSummaries();
        operatorSummaries.forEach(f -> {
            System.out.println(f.getOperatorType());
        });
        List<OperatorStats> operators = operatorSummaries.stream()
                .filter(f -> f.getOperatorType().equals("TableScanOperator")).toList();
        if (operators.isEmpty()) {
            fail("Plan should have a TableScanOperator");
        }
        else {
            OperatorStats operatorStats = operators.get(0);
            //Check if operator scanned 10 rows
            assertEquals(operatorStats.getInputPositions(), expectedInputRows);
            //Check if operator returned 10 rows
            assertEquals(operatorStats.getOutputPositions(), expectedOutputRows);
        }
    }
}
