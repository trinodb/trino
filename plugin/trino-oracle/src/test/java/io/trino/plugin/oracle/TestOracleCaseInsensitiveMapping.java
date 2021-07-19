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

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static java.lang.String.format;

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

    @Override
    protected AutoCloseable withSchema(String schemaName)
    {
        onRemoteDatabase().execute(format("CREATE USER %s IDENTIFIED BY SCM", quoted(schemaName)));
        onRemoteDatabase().execute(format("ALTER USER %s QUOTA 100M ON SYSTEM", quoted(schemaName)));
        return () -> onRemoteDatabase().execute("DROP USER " + quoted(schemaName));
    }

    @Override
    protected AutoCloseable withTable(String remoteSchemaName, String remoteTableName, String tableDefinition)
    {
        String quotedName = quoted(remoteSchemaName) + "." + quoted(remoteTableName);

        onRemoteDatabase().execute(format("CREATE USER %s IDENTIFIED BY SCM", quoted(remoteSchemaName)));
        onRemoteDatabase().execute(format("ALTER USER %s QUOTA 100M ON SYSTEM", quoted(remoteSchemaName)));
        onRemoteDatabase().execute(format("CREATE TABLE %s %s", quotedName, tableDefinition));
        onRemoteDatabase().execute("DROP USER " + quoted(remoteSchemaName));
        return () -> onRemoteDatabase().execute("DROP TABLE " + quotedName);
    }

}
