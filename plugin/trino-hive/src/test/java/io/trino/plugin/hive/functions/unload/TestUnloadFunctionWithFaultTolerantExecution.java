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
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnloadFunctionWithFaultTolerantExecution
        extends AbstractTestQueryFramework
{
    private final Path tempDir = createTempDir();

    private static Path createTempDir()
    {
        try {
            return Files.createTempDirectory("trino-unload-fte-test");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION))
                .addHiveProperty("hive.unload-enabled", "true")
                .addExtraProperty("retry-policy", "TASK")
                .withExchange("filesystem")
                .setWorkerCount(0)
                .build();
    }

    @Test
    public void testUnloadWithFaultTolerantExecution()
    {
        String outputDir = tempDir.resolve("unload_fte").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isGreaterThan(0);
    }

    @Test
    public void testUnloadMultipleFormatsWithFaultTolerantExecution()
    {
        for (String format : new String[] {"PARQUET", "ORC", "AVRO"}) {
            String outputDir = tempDir.resolve("unload_fte_" + format.toLowerCase(Locale.ROOT)).toUri().toString();
            MaterializedResult result = computeActual(
                    "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                            "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                            "location => '" + outputDir + "', " +
                            "format => '" + format + "'))");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25);
        }
    }
}
