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
package io.trino.plugin.cockroachdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseCaseInsensitiveMappingTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static io.trino.plugin.jdbc.mapping.RuleBasedIdentifierMappingUtils.createRuleBasedIdentifierMappingFile;
import static java.util.Objects.requireNonNull;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestCockroachDbCaseInsensitiveMapping
        extends BaseCaseInsensitiveMappingTest
{
    private Path mappingFile;
    private TestingCockroachDbServer cockroachDbServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mappingFile = createRuleBasedIdentifierMappingFile();
        cockroachDbServer = closeAfterClass(new TestingCockroachDbServer());
        return CockroachDbQueryRunner.createCockroachDbQueryRunner(
                cockroachDbServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .put("case-insensitive-name-matching.config-file", mappingFile.toFile().getAbsolutePath())
                        .put("case-insensitive-name-matching.config-file.refresh-period", "1ms") // ~always refresh
                        .build(),
                ImmutableSet.of());
    }

    @Override
    protected Path getMappingFile()
    {
        return requireNonNull(mappingFile, "mappingFile is null");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return requireNonNull(cockroachDbServer, "cockroachDbServer is null")::execute;
    }
}
