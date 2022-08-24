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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestSqlServerCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        sqlServer = closeAfterClass(new TestingSqlServer());
        return createSqlServerQueryRunner(
                sqlServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
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
    protected SqlExecutor onRemoteDatabase()
    {
        return requireNonNull(sqlServer, "sqlServer is null")::execute;
    }

    @Override
    public void testNonLowerCaseSchemaNameCreateTableAsSelect()
            throws Exception
    {
        // TODO: Fix CTAS in non-lowercase schemas to work with case-insensitive name matching enabled (https://github.com/trinodb/trino/issues/13826)
        try (AutoCloseable ignore1 = withSchema("NonLowerCaseSchema")) {
            assertQueryFails("CREATE TABLE nonlowercaseschema.ctas_table AS SELECT 'a' c", "\\QThis connector does not support renaming tables across schemas\\E");
        }
    }

    @Test
    public void testSqlServerCollation()
            throws Exception
    {
        try (Connection connection = sqlServer.createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT SERVERPROPERTY ('Collation')")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo("Latin1_General_CS_AS");
            assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    public void forceTestNgToRespectSingleThreaded()
    {
        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
    }
}
