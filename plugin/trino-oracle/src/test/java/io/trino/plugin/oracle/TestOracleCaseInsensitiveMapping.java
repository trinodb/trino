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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestOracleCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private TestingOracleServer oracleServer;
    private Path mappingFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.oracleServer = closeAfterClass(new TestingOracleServer());
        this.mappingFile = createRuleBasedIdentifierMappingFile();
        return createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .putAll(OracleQueryRunner.connectionProperties(oracleServer))
                        .put("case-insensitive-name-matching", "true")
                        .build(),
                ImmutableList.of());
    }

    @Override
    protected Path getMappingFile()
    {
        return mappingFile;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return oracleServer::execute;
    }

    @Test
    @Override
    //Oracle Tests need to use TestingOracleServer.TEST_USER schema, so some methods have been overriden 
    public void testTableNameClash()
            throws Exception
    {
        String[] nameVariants = {"casesensitivename", "CaseSensitiveName", "CASESENSITIVENAME"};
        assertThat(Stream.of(nameVariants)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet()))
                .hasSize(1);

        for (int i = 0; i < nameVariants.length; i++) {
            for (int j = i + 1; j < nameVariants.length; j++) {
                try (AutoCloseable ignore1 = withTable(TestingOracleServer.TEST_USER, nameVariants[i], "(c varchar(5))");
                        AutoCloseable ignore2 = withTable(TestingOracleServer.TEST_USER, nameVariants[j], "(d varchar(5))");
                        AutoCloseable ignore3 = withTable(TestingOracleServer.TEST_USER, "some_table", "(d varchar(5))")) {
                    assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("casesensitivename"::equals)).hasSize(1);
                    assertQueryFails("SHOW COLUMNS FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                    assertQueryFails("SELECT * FROM casesensitivename", "Failed to find remote table name: Ambiguous name: casesensitivename");
                }
            }
        }
    }

    @Override
    protected AutoCloseable withSchema(String schemaName)
    {
        onRemoteDatabase().execute(format("CREATE USER %s IDENTIFIED BY SCM", quoted(schemaName)));
        onRemoteDatabase().execute(format("ALTER USER %s QUOTA 100M ON SYSTEM", quoted(schemaName)));
        return () -> onRemoteDatabase().execute("DROP USER " + quoted(schemaName));
    }
}
