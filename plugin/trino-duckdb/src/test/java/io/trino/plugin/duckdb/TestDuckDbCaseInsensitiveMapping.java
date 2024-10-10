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
package io.trino.plugin.duckdb;

import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.REFRESH_PERIOD_DURATION;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckDbCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingDuckDb duckDb;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        duckDb = closeAfterClass(new TestingDuckDb());
        return DuckDbQueryRunner.builder(duckDb)
                .addConnectorProperty("case-insensitive-name-matching", "true")
                .addConnectorProperty("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                .addConnectorProperty("case-insensitive-name-matching.config-file.refresh-period", REFRESH_PERIOD_DURATION.toString())
                .build();
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return duckDb::execute;
    }

    @Test
    @Override
    public void testSchemaNameClash()
    {
        assertThatThrownBy(super::testSchemaNameClash)
                .hasMessageContaining("Failed to execute statement 'CREATE SCHEMA \"CaseSensitiveName\"'")
                .hasStackTraceContaining("Schema with name \"CaseSensitiveName\" already exists");
    }

    @Test
    @Override
    public void testSchemaNameClashWithRuleMapping()
    {
        assertThatThrownBy(super::testSchemaNameClashWithRuleMapping)
                .hasMessageContaining("Failed to execute statement 'CREATE SCHEMA \"CaseSensitiveName\"'")
                .hasStackTraceContaining("Schema with name \"CaseSensitiveName\" already exists");
    }

    @Test
    @Override
    public void testTableNameClash()
    {
        assertThatThrownBy(super::testTableNameClash)
                .hasMessageContaining("Failed to execute statement 'CREATE TABLE \"tpch\".\"CaseSensitiveName\"")
                .hasStackTraceContaining("Table with name \"CaseSensitiveName\" already exists");
    }

    @Test
    @Override
    public void testTableNameClashWithRuleMapping()
    {
        assertThatThrownBy(super::testTableNameClashWithRuleMapping)
                .hasMessageContaining("Failed to execute statement 'CREATE TABLE \"remote_schema\".\"CaseSensitiveName\" (d varchar(5))'")
                .hasStackTraceContaining("Table with name \"CaseSensitiveName\" already exists");
    }
}
