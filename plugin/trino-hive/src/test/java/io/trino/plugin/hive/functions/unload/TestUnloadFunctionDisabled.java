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
package io.trino.plugin.hive.functions.unload;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.tpch.TpchTable.NATION;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnloadFunctionDisabled
        extends AbstractTestQueryFramework
{
    private final Path tempDir = createTempDir();

    private static Path createTempDir()
    {
        try {
            return Files.createTempDirectory("trino-unload-disabled-test");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Default config: hive.unload-enabled is false
        return HiveQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION))
                .setWorkerCount(0)
                .build();
    }

    @Test
    public void testUnloadDisabledByDefault()
    {
        String outputDir = tempDir.resolve("unload_disabled").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))",
                ".*unload table function is disabled.*");
    }
}
