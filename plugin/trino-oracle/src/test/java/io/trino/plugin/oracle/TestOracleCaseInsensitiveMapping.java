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
import java.util.Optional;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestOracleCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        oracleServer = closeAfterClass(new TestingOracleServer());
        return createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .putAll(OracleQueryRunner.connectionProperties(oracleServer))
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", REFRESH_PERIOD_DURATION.toString())
                        .buildOrThrow(),
                ImmutableList.of());
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected Optional<String> optionalFromDual()
    {
        return Optional.of("FROM dual");
    }

    @Override
    protected AutoCloseable withSchema(String schemaName)
    {
        onRemoteDatabase().execute(format("CREATE USER %s IDENTIFIED BY SCM", quoted(schemaName)));
        onRemoteDatabase().execute(format("GRANT UNLIMITED TABLESPACE TO %s", quoted(schemaName)));
        return () -> onRemoteDatabase().execute("DROP USER " + quoted(schemaName));
    }

    @Override
    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String schemaName = quoted(remoteSchemaName);
        // The TEST_USER is created without quoting in TestingOracleServer#createConfigureScript, quoting it here causes ORA-01918: user 'trino_test' does not exist
        if (remoteSchemaName.equalsIgnoreCase(TEST_USER)) {
            schemaName = remoteSchemaName;
        }
        String quotedName = schemaName + "." + quoted(remoteTableName);
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        return () -> onRemoteDatabase().execute("DROP TABLE " + quotedName);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return requireNonNull(oracleServer, "oracleServer is null")::execute;
    }

    @Test
    public void forceTestNgToRespectSingleThreaded()
    {
        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
    }
}
