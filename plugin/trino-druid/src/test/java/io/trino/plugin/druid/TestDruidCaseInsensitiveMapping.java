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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.plugin.jdbc.mapping.TableMappingRule;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchDataFromSourceToTarget;
import static io.trino.plugin.druid.DruidQueryRunner.createDruidQueryRunnerTpch;
import static io.trino.plugin.druid.DruidTpchTables.SELECT_FROM_ORDERS;
import static io.trino.plugin.druid.DruidTpchTables.SELECT_FROM_REGION;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDruidCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private TestingDruidServer druidServer;
    private Path mappingFile;

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (druidServer != null) {
            druidServer.close();
            druidServer = null;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        druidServer = new TestingDruidServer();
        mappingFile = createRuleBasedIdentifierMappingFile();
        DistributedQueryRunner queryRunner = createDruidQueryRunnerTpch(
                druidServer,
                ImmutableMap.of(),
                ImmutableMap.of("case-insensitive-name-matching", "true",
                        "case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath(),
                        "case-insensitive-name-matching.config-file.refresh-period", "1ms"), // ~always refresh
                ImmutableList.of(ORDERS, REGION));
        copyAndIngestTpchDataFromSourceToTarget(queryRunner.execute(SELECT_FROM_ORDERS + " LIMIT 10"), this.druidServer, "orders", "MiXeD_CaSe", Optional.empty());

        return queryRunner;
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return druidServer::execute;
    }

    @Override
    @Test
    public void testNonLowerCaseTableName()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE MiXeD_CaSe");
        assertThat(actualColumns).isEqualTo(expectedColumns);
        assertQuery("SELECT COUNT(1) FROM druid.druid.mixed_case", "VALUES 10");
        assertQuery("SELECT COUNT(1) FROM druid.druid.MIXED_CASE", "VALUES 10");
    }

    @Override
    @Test
    public void testTableNameClash()
            throws Exception
    {
        copyAndIngestTpchDataFromSourceToTarget(
                getQueryRunner().execute(SELECT_FROM_REGION),
                this.druidServer,
                "region",
                "CaseSensitiveName",
                Optional.empty());
        copyAndIngestTpchDataFromSourceToTarget(
                getQueryRunner().execute(SELECT_FROM_REGION),
                this.druidServer,
                "region",
                "casesensitivename",
                // when you create a second datasource with the same name (ignoring case) the filename mentioned in firehose's ingestion config, must match with the first one.
                // Otherwise druid's loadstatus (and system tables) fails to register the second datasource created.
                Optional.of("CaseSensitiveName"));

        assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1);
        assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
        assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
    }

    @Override
    @Test
    public void testTableNameRuleMapping()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(),
                ImmutableList.of(new TableMappingRule("druid", "remote_table", "trino_table")));

        copyAndIngestTpchDataFromSourceToTarget(getQueryRunner().execute(SELECT_FROM_REGION), this.druidServer, "region", "remote_table", Optional.empty());

        assertThat(computeActual("SHOW TABLES FROM druid").getOnlyColumn())
                .contains("trino_table");
        assertQuery("SELECT COUNT(1) FROM druid.druid.trino_table", "VALUES 5");
    }

    @Override
    @Test
    public void testTableNameClashWithRuleMapping()
            throws Exception
    {
        String schema = "druid";
        List<TableMappingRule> tableMappingRules = ImmutableList.of(
                new TableMappingRule(schema, "CaseSensitiveName", "casesensitivename_a"),
                new TableMappingRule(schema, "casesensitivename", "casesensitivename_b"));
        updateRuleBasedIdentifierMappingFile(getMappingFile(), ImmutableList.of(), tableMappingRules);

        copyAndIngestTpchDataFromSourceToTarget(
                getQueryRunner().execute(SELECT_FROM_REGION),
                this.druidServer,
                "region",
                "CaseSensitiveName",
                Optional.empty());
        copyAndIngestTpchDataFromSourceToTarget(
                getQueryRunner().execute(SELECT_FROM_REGION),
                this.druidServer,
                "region",
                "casesensitivename",
                // when you create a second datasource with the same name (ignoring case) the filename mentioned in firehose's ingestion config, must match with the first one.
                // Otherwise druid's loadstatus (and system tables) fails to register the second datasource created.
                Optional.of("CaseSensitiveName"));

        assertThat(computeActual("SHOW TABLES FROM druid")
                .getOnlyColumn()
                .map(String.class::cast)
                .filter(anObject -> anObject.startsWith("casesensitivename")))
                .hasSize(2);
        assertQuery("SELECT COUNT(1) FROM druid.druid.casesensitivename_a", "VALUES 5");
        assertQuery("SELECT COUNT(1) FROM druid.druid.casesensitivename_b", "VALUES 5");
    }

    @Override
    @Test
    public void testNonLowerCaseSchemaName()
    {
        // related to https://github.com/trinodb/trino/issues/14700
        throw new SkipException("Druid connector only supports schema 'druid'.");
    }

    @Override
    @Test
    public void testSchemaAndTableNameRuleMapping()
    {
        // related to https://github.com/trinodb/trino/issues/14700
        throw new SkipException("Druid connector only supports schema 'druid'.");
    }

    @Override
    @Test
    public void testSchemaNameClash()
    {
        // related to https://github.com/trinodb/trino/issues/14700
        throw new SkipException("Druid connector only supports schema 'druid'.");
    }

    @Override
    @Test
    public void testSchemaNameClashWithRuleMapping()
    {
        // related to https://github.com/trinodb/trino/issues/14700
        throw new SkipException("Druid connector only supports schema 'druid'.");
    }

    @Override
    @Test
    public void testSchemaNameRuleMapping()
    {
        // related to https://github.com/trinodb/trino/issues/14700
        throw new SkipException("Druid connector only supports schema 'druid'.");
    }
}
