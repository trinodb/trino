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
package io.trino.plugin.jdbc;

import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;

import java.nio.file.Path;

import static io.trino.plugin.base.mapping.testing.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static java.util.Objects.requireNonNull;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
public class TestJdbcCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingH2JdbcServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        server = closeAfterClass(new TestingH2JdbcServer());
        return createH2QueryRunner();
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
        String identifierQuote = "\"";
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }
}
