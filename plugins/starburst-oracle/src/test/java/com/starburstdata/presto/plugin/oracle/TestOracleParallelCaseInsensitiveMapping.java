/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestOracleParallelCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("case-insensitive-name-matching", "true")
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.parallel.max-splits-per-scan", "17")
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testNonLowerCaseSchemaName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"NonLowerCaseSchema\"");
                AutoCloseable ignore2 = withPartitionedTable("\"NonLowerCaseSchema\".lower_case_name", "c varchar(5)", "c", 3);
                AutoCloseable ignore3 = withPartitionedTable("\"NonLowerCaseSchema\".\"Mixed_Case_Name\"", "c varchar(5)", "c", 3);
                AutoCloseable ignore4 = withPartitionedTable("\"NonLowerCaseSchema\".\"UPPER_CASE_NAME\"", "c varchar(5)", "c", 3)) {
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains("nonlowercaseschema");
            assertQuery("SHOW SCHEMAS LIKE 'nonlowerc%'", "VALUES 'nonlowercaseschema'");
            assertQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%nonlowercaseschema'", "VALUES 'nonlowercaseschema'");
            assertQuery("SHOW TABLES FROM nonlowercaseschema", "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'nonlowercaseschema'", "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQueryReturnsEmptyResult("SELECT * FROM nonlowercaseschema.lower_case_name");
        }
    }

    @Test
    public void testNonLowerCaseTableName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"SomeSchema\"");
                AutoCloseable ignore2 = withPartitionedTable(
                        "\"SomeSchema\".\"NonLowerCaseTable\"", "\"lower_case_name\" varchar(5), \"Mixed_Case_Name\" varchar(5), \"UPPER_CASE_NAME\" varchar(5), partcol varchar(5)", "partcol", 3)) {
            assertUpdate("INSERT INTO someschema.nonlowercasetable VALUES ('a', 'b', 'c', 'd')", 1);

            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'someschema' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name', 'partcol'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name', 'partcol'");
            assertEquals(
                    computeActual("SHOW COLUMNS FROM someschema.nonlowercasetable").getMaterializedRows().stream()
                            .map(row -> row.getField(0))
                            .collect(toImmutableSet()),
                    ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name", "partcol"));

            // Note: until https://github.com/prestodb/presto/issues/2863 is resolved, this is *the* way to access the tables.

            assertQuery("SELECT lower_case_name FROM someschema.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM someschema.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM someschema.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM SomeSchema.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"SomeSchema\".\"NonLowerCaseTable\"", "VALUES 'c'");

            assertUpdate("INSERT INTO someschema.nonlowercasetable (lower_case_name) VALUES ('l')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (mixed_case_name) VALUES ('m')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (upper_case_name) VALUES ('u')", 1);
            assertQuery(
                    "SELECT * FROM someschema.nonlowercasetable",
                    "VALUES ('a', 'b', 'c', 'd')," +
                            "('l', NULL, NULL, NULL)," +
                            "(NULL, 'm', NULL, NULL)," +
                            "(NULL, NULL, 'u', NULL)");
        }
    }

    @Test
    public void testSchemaNameClash()
            throws Exception
    {
        String[] nameVariants = {"\"casesensitivename\"", "\"CaseSensitiveName\"", "\"CASESENSITIVENAME\""};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                String schemaName = nameVariants[i];
                String otherSchemaName = nameVariants[j];
                try (AutoCloseable ignore1 = withSchema(schemaName);
                        AutoCloseable ignore2 = withSchema(otherSchemaName);
                        AutoCloseable ignore3 = withPartitionedTable(schemaName + ".some_table_name", "c varchar(5)", "c", 3)) {
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn()).contains("casesensitivename");
                    assertThat(computeActual("SHOW SCHEMAS").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO change io.trino.plugin.jdbc.JdbcClient.getSchemaNames to return a List
                    assertQueryFails("SHOW TABLES FROM casesensitivename", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename.some_table_name", "Failed to find remote schema name: Ambiguous name: casesensitivename");
                }
            }
        }
    }

    @Test
    public void testTableNameClash()
            throws Exception
    {
        String[] nameVariants = {"\"casesensitivename\"", "\"CaseSensitiveName\"", "\"CASESENSITIVENAME\""};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.replace("\"", "").toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = withPartitionedTable(OracleTestUsers.USER + "." + nameVariants[i], "c varchar(5)", "c", 3);
                        AutoCloseable ignore2 = withPartitionedTable(OracleTestUsers.USER + "." + nameVariants[j], "d varchar(5)", "d", 3)) {
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn()).contains("casesensitivename");
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1); // TODO, should be 2
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                }
            }
        }
    }

    private AutoCloseable withSchema(String schemaName)
    {
        executeInOracle(format("CREATE USER %s IDENTIFIED BY SCM", schemaName));
        executeInOracle(format("ALTER USER %s QUOTA 100M ON USERS", schemaName));
        return () -> executeInOracle("DROP USER " + schemaName);
    }

    private AutoCloseable withPartitionedTable(String tableName, String columns, String partitionColumn, int partitionCount)
    {
        executeInOracle(format("CREATE TABLE %s (%s) PARTITION BY HASH (%s) PARTITIONS %d", tableName, columns, partitionColumn, partitionCount));
        return () -> executeInOracle(format("DROP TABLE %s", tableName));
    }
}
