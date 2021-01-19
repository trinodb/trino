/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static io.prestosql.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestSapHanaColumnViews
        extends AbstractTestQueryFramework
{
    protected TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        // SAP Hana creates mixed case names for column views by default
                        .put("case-insensitive-name-matching", "true")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(NATION));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Test
    public void testSelectFromCalculatedView()
            throws Exception
    {
        // We use slash in the table name is it convention SAP HANA uses when views are being
        // activated based on object model. The part before slash denotes package in which view is defined
        // and part after slash actual view name.
        String viewName = "views/calc_view_" + randomTableSuffix();
        server.execute("CREATE CALCULATION SCENARIO " +
                "\"_SYS_BIC\".\"" + viewName + "\" USING " +
                "'[\n" +
                        "   {\n" +
                        "      \"__CalculationNode__\":true,\n" +
                        "      \"name\":\"NATION\",\n" +
                        "      \"operation\":{\n" +
                        "         \"__TableDSNodeData__\":true,\n" +
                        "         \"source\":{\n" +
                        "            \"__IndexName__\":true,\n" +
                        "            \"schema\":\"TPCH\",\n" +
                        "            \"name\":\"NATION\"\n" +
                        "         },\n" +
                        "         \"dataSourceFlags\":0\n" +
                        "      },\n" +
                        "      \"attributeVec\":[\n" +
                        "         {\n" +
                        "            \"__Attribute__\":true,\n" +
                        "            \"name\":\"NATIONKEY\",\n" +
                        "            \"role\":1,\n" +
                        "            \"datatype\":{\n" +
                        "               \"__DataType__\":true,\n" +
                        "               \"type\":66,\n" +
                        "               \"length\":18,\n" +
                        "               \"sqlType\":4,\n" +
                        "               \"sqlLength\":9\n" +
                        "            },\n" +
                        "            \"attributeType\":0\n" +
                        "         }\n" +
                        "      ]\n" +
                        "   },\n" +
                        "   {\n" +
                        "      \"__CalculationNode__\":true,\n" +
                        "      \"name\":\"finalProjection\",\n" +
                        "      \"isDefaultNode\":true,\n" +
                        "      \"inputVec\":[\n" +
                        "         {\n" +
                        "            \"__Input__\":true,\n" +
                        "            \"name\":\"NATION\",\n" +
                        "            \"mappingVec\":[\n" +
                        "               {\n" +
                        "                  \"__Mapping__\":true,\n" +
                        "                  \"type\":1,\n" +
                        "                  \"target\":\"NATIONKEY\",\n" +
                        "                  \"source\":\"NATIONKEY\",\n" +
                        "                  \"length\":0\n" +
                        "               }\n" +
                        "            ]\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"operation\":{\n" +
                        "         \"__ProjectionOpNodeData__\":true\n" +
                        "      },\n" +
                        "      \"attributeVec\":[\n" +
                        "         {\n" +
                        "            \"__Attribute__\":true,\n" +
                        "            \"name\":\"NATIONKEY\",\n" +
                        "            \"role\":1,\n" +
                        "            \"datatype\":{\n" +
                        "               \"__DataType__\":true,\n" +
                        "               \"type\":66,\n" +
                        "               \"length\":18,\n" +
                        "               \"sqlType\":4,\n" +
                        "               \"sqlLength\":9\n" +
                        "            },\n" +
                        "            \"description\":\"NATIONKEY\",\n" +
                        "            \"attributeType\":0\n" +
                        "         }\n" +
                        "      ],\n" +
                        "      \"debugNodeDataInfo\":{\n" +
                        "         \"__DebugNodeDataInfo__\":true,\n" +
                        "         \"nodeName\":\"Projection\"\n" +
                        "      }\n" +
                        "   },\n" +
                        "   {\n" +
                        "      \"__Variable__\":true,\n" +
                        "      \"name\":\"$$language$$\",\n" +
                        "      \"typeMask\":512,\n" +
                        "      \"usage\":0,\n" +
                        "      \"isGlobal\":true\n" +
                        "   },\n" +
                        "   {\n" +
                        "      \"__Variable__\":true,\n" +
                        "      \"name\":\"$$client$$\",\n" +
                        "      \"typeMask\":512,\n" +
                        "      \"usage\":0,\n" +
                        "      \"isGlobal\":true\n" +
                        "   },\n" +
                        "   {\n" +
                        "      \"__CalcScenarioMetaData__\":true,\n" +
                        "      \"externalScenarioName\":\"not::important\"\n" +
                        "   }\n" +
                        "]'");

        // Despite saying COLUMN VIEW it actually creates CALCULATED VIEW (reported as CALC VIEW in JDBC metadata); actual view type is determined by parameters.
        server.execute("CREATE COLUMN VIEW \"_SYS_BIC\".\"" + viewName + "\" WITH PARAMETERS (indexType=11,\n" +
                "\t 'PARENTCALCINDEXSCHEMA'='_SYS_BIC',\n" +
                "\t'PARENTCALCINDEX'='" + viewName + "',\n" +
                "\t'PARENTCALCNODE'='finalProjection')");

        assertThat(getViewType(viewName)).isEqualTo("CALC");

        String testQuery = "SELECT * FROM _SYS_BIC.\"" + viewName + "\"";
        assertThat(query(testQuery)).matches("SELECT nationkey FROM tpch.tiny.nation");

        Session session = Session.builder(getSession())
                .addPreparedStatement("test_query", testQuery)
                .build();
        MaterializedResult expected = MaterializedResult.resultBuilder(session, createVarcharType(9), createVarcharType(7), createVarcharType(8), createVarcharType(21), createVarcharType(6), INTEGER, BOOLEAN)
                .row("nationkey", "saphana", "_sys_bic", viewName, "bigint", 8, false)
                .build();
        assertThat(query(session, "DESCRIBE OUTPUT test_query")).containsAll(expected);

        assertThat(computeActual("SHOW TABLES FROM _SYS_BIC").getOnlyColumnAsSet()).contains(viewName);
    }

    private String getViewType(String viewName)
            throws Exception
    {
        try (Connection connection = DriverManager.getConnection(server.getJdbcUrl(), server.getUser(), server.getPassword());
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT VIEW_TYPE FROM SYS.VIEWS WHERE SCHEMA_NAME = '_SYS_BIC' AND VIEW_NAME = '" + viewName + "'");
            if (!resultSet.next()) {
                throw new RuntimeException("VIEW " + viewName + " not found");
            }
            return resultSet.getString(1);
        }
    }
}
