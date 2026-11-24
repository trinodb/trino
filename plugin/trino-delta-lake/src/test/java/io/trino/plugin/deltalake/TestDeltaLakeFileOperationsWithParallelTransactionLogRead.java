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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.Session;
import io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileOperation;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.CHECKSUM;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.DATA;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.deltalake.TestDeltaLakeFileOperations.getOperations;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;

// single-threaded as DistributedQueryRunner.spans is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
final class TestDeltaLakeFileOperationsWithParallelTransactionLogRead
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.transaction-log.parallel-read.enabled", "true")
                .build();
    }

    @Test
    public void testSelectLatestVersion()
    {
        String tableName = "test_select_latest_version_" + randomNameSuffix();
        createTableWithCommits(tableName, 3);

        // One existence check and one listing replace the length check of every commit
        assertFileSystemAccesses(
                "SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(CHECKSUM, "00000000000000000003.crc", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .addCopies(new FileOperation(TRANSACTION_LOG_DIRECTORY, "_delta_log", "FileSystem.listFilesStartingFrom"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 3)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSelectLatestVersionWithParallelReadDisabledInSession()
    {
        String tableName = "test_select_latest_version_disabled_" + randomNameSuffix();
        createTableWithCommits(tableName, 3);

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "transaction_log_parallel_read_enabled", "false")
                .build();
        assertFileSystemAccesses(
                session,
                "SELECT * FROM " + tableName,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(CHECKSUM, "00000000000000000003.crc", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_DIRECTORY, "_delta_log", "FileSystem.listFilesStartingFrom"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000004.json", "InputFile.length"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 3)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSelectPastCheckpointVersion()
    {
        String tableName = "test_select_past_checkpoint_version_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(id int) WITH (checkpoint_interval = 2)");
        for (int i = 0; i < 6; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES " + i, 1);
        }

        // The listing stops at the end version, before the later commits
        assertFileSystemAccesses(
                "SELECT * FROM " + tableName + " FOR VERSION AS OF 3",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", "InputFile.newStream"))
                        .add(new FileOperation(CHECKSUM, "00000000000000000003.crc", "InputFile.newStream"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.exists"))
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.length"), 2)
                        .addCopies(new FileOperation(CHECKPOINT, "00000000000000000002.checkpoint.parquet", "InputFile.newInput"), 2)
                        .add(new FileOperation(TRANSACTION_LOG_DIRECTORY, "_delta_log", "FileSystem.listFilesStartingFrom"))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", "InputFile.newStream"))
                        .addCopies(new FileOperation(DATA, "no partition", "InputFile.newInput"), 3)
                        .build());

        assertUpdate("DROP TABLE " + tableName);
    }

    private void createTableWithCommits(String tableName, int inserts)
    {
        assertUpdate("CREATE TABLE " + tableName + "(id int)");
        for (int i = 0; i < inserts; i++) {
            assertUpdate("INSERT INTO " + tableName + " VALUES " + i, 1);
        }
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");

        getDistributedQueryRunner().executeWithPlan(session, query);
        List<SpanData> spanData = getDistributedQueryRunner().getSpans();
        assertMultisetsEqual(getOperations(spanData, operation -> operation.equals("FileSystem.listFilesStartingFrom")), expectedAccesses);
    }
}
