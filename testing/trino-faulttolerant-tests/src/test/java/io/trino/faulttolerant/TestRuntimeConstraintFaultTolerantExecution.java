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
package io.trino.faulttolerant;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

public class TestRuntimeConstraintFaultTolerantExecution
        extends BaseFaultTolerantExecutionTest
{
    public TestRuntimeConstraintFaultTolerantExecution()
    {
        super("partitioning");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path exchangeDirectory = createTempDirectory("runtime-constraint-exchange");
        closeAfterClass(() -> deleteRecursively(exchangeDirectory, ALLOW_INSECURE));
        return IcebergQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .addExtraProperty("legacy-dynamic-filtering", "false")
                .withExchange("filesystem", ImmutableMap.of(
                        "exchange.base-directories", exchangeDirectory.toUri().toString(),
                        "exchange.sink-max-file-size", "16MB",
                        "exchange.source-handle-target-data-size", "1MB"))
                .build();
    }

    @Test
    void testMergeAfterWiringInitialization()
    {
        testMergeAfterWiringInitialization(1, "");
        testMergeAfterWiringInitialization(4, "");
        testMergeAfterWiringInitialization(1, ", partitioning = ARRAY['bucket(id, 3)']");
        testMergeAfterWiringInitialization(4, ", partitioning = ARRAY['bucket(id, 3)']");
    }

    private void testMergeAfterWiringInitialization(int writers, String partitioning)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("task_min_writer_count", Integer.toString(writers))
                .setSystemProperty("task_max_writer_count", Integer.toString(writers))
                .build();
        try (TestTable table = newTrinoTable("test_wiring_merge", "(id BIGINT, value BIGINT) WITH (format_version = 2" + partitioning + ")")) {
            String original = range(1, 32).mapToObj(id -> "(%s, 1000)".formatted(id)).collect(joining(", "));
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " + original, 31);
            String firstSource = range(16, 32).mapToObj(id -> "(%s, 3000)".formatted(id)).collect(joining(", "));
            assertUpdate(session, "MERGE INTO " + table.getName() + " t USING (VALUES " + firstSource + ") s(id, value) " +
                    "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value", 16);
            String unrelated = range(32, 48).mapToObj(id -> "(%s, 4000)".formatted(-id)).collect(joining(", "));
            assertUpdate("INSERT INTO " + table.getName() + " VALUES " + unrelated, 16);
            String secondSource = range(1, 48).mapToObj(id -> "(%s, 5000)".formatted(id)).collect(joining(", "));
            assertUpdate(session, "MERGE INTO " + table.getName() + " t USING (VALUES " + secondSource + ") s(id, value) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED AND t.value = 1000 THEN DELETE " +
                    "WHEN MATCHED THEN UPDATE SET value = 6000 " +
                    "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)", 47);
            String updated = range(16, 32).mapToObj(id -> "(%s, 6000)".formatted(id)).collect(joining(", "));
            String inserted = range(32, 48).mapToObj(id -> "(%s, 5000)".formatted(id)).collect(joining(", "));
            assertQuery("SELECT id, value FROM " + table.getName(), "VALUES " + updated + ", " + inserted + ", " + unrelated);
        }
    }
}
