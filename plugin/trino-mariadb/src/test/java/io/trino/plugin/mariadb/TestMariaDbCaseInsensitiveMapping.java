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
package io.trino.plugin.mariadb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.mapping.ColumnMappingRule;
import io.trino.plugin.base.mapping.SchemaMappingRule;
import io.trino.plugin.base.mapping.TableMappingRule;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.base.mapping.RuleBasedIdentifierMappingUtils.updateRuleBasedIdentifierMappingFile;
import static io.trino.plugin.mariadb.MariaDbQueryRunner.createMariaDbQueryRunner;
import static java.util.Objects.requireNonNull;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
public class TestMariaDbCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingMariaDbServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        server = closeAfterClass(new TestingMariaDbServer());
        return createMariaDbQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", "1ms") // ~always refresh
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
        return server::execute;
    }

    @Override
    protected String quoted(String name)
    {
        String identifierQuote = "`";
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    @Override
    @Test
    public void testSchemaAndTableMappingsWithColumnMappings()
            throws Exception
    {
        updateRuleBasedIdentifierMappingFile(
                getMappingFile(),
                ImmutableList.of(new SchemaMappingRule("RemoteSchema", "remote_schema")),
                ImmutableList.of(new TableMappingRule("RemoteSchema", "RemoteTable", "remote_table")),
                ImmutableList.of(
                        new ColumnMappingRule("RemoteSchema", "RemoteTable", "col1", "c1"),
                        new ColumnMappingRule("RemoteSchema", "RemoteTable", "col2", "c2")));

        try (AutoCloseable ignore1 = withSchema("RemoteSchema");
                AutoCloseable ignore2 = withTable("RemoteSchema", "RemoteTable", "(" + quoted("col2") + " varchar(5), " + quoted("col1") + " int)")) {
            assertTableColumnNames("remote_schema.remote_table", "c2", "c1");
            assertUpdate("INSERT INTO remote_schema.remote_table VALUES ('a', 1)", 1);
            assertUpdate("INSERT INTO remote_schema.remote_table (c2, c1) VALUES ('b', 2)", 1);
            assertUpdate("INSERT INTO remote_schema.remote_table (c2) VALUES ('c')", 1);

            assertQuery("SELECT * FROM remote_schema.remote_table", "VALUES ('a', 1), ('b', 2), ('c', null)");
            assertQuery("SELECT c2, c1 FROM remote_schema.remote_table", "VALUES ('a', 1), ('b', 2), ('c', null)");
            assertUpdate("DELETE FROM remote_schema.remote_table where c1 = 2", 1);
            assertQuery("SELECT c2 FROM remote_schema.remote_table", "VALUES 'a', 'c'");
        }
    }
}
