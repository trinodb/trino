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
package io.trino.plugin.memsql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static io.trino.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestMemSqlCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    protected TestingMemSqlServer memSqlServer;
    protected Path mappingFile;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.memSqlServer = new TestingMemSqlServer();
        this.mappingFile = createRuleBasedIdentifierMappingFile();
        return createMemSqlQueryRunner(memSqlServer, ImmutableMap.of("case-insensitive-name-matching", "true"), ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        memSqlServer.close();
    }

    @Override
    protected Path getMappingFile()
    {
        return mappingFile;
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sql -> memSqlServer.execute(sql);
    }
}
