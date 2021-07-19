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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestBigQueryCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private BigQuerySqlExecutor bigQuerySqlExecutor;
    private Path mappingFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
        this.mappingFile = createRuleBasedIdentifierMappingFile();
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("bigquery.case-insensitive-name-matching", "true"));
    }

    @Override
    protected Path getMappingFile()
    {
        return mappingFile;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return bigQuerySqlExecutor;
    }

    @Test
    public void testDropSchema()
    {
        String schema = "Test_Drop_Case_Sensitive";
        bigQuerySqlExecutor.execute(format("DROP SCHEMA IF EXISTS `%s`", schema));
        bigQuerySqlExecutor.execute(format("CREATE SCHEMA `%s`", schema));
        assertUpdate("DROP SCHEMA " + schema.toLowerCase(ENGLISH));
    }

    @Test
    public void testDropSchemaNameClash()
    {
        String schema = "Test_Drop_Case_Sensitive_Clash";
        bigQuerySqlExecutor.execute(format("DROP SCHEMA IF EXISTS `%s`", schema));
        bigQuerySqlExecutor.execute(format("DROP SCHEMA IF EXISTS `%s`", schema.toLowerCase(ENGLISH)));
        bigQuerySqlExecutor.execute(format("CREATE SCHEMA `%s`", schema));
        bigQuerySqlExecutor.execute(format("CREATE SCHEMA `%s`", schema.toLowerCase(ENGLISH)));
        assertQueryFails("DROP SCHEMA " + schema.toLowerCase(ENGLISH), "Found ambiguous names in BigQuery.*");
        bigQuerySqlExecutor.execute(format("DROP SCHEMA `%s`", schema));
        bigQuerySqlExecutor.execute(format("DROP SCHEMA `%s`", schema.toLowerCase(ENGLISH)));
    }
}
