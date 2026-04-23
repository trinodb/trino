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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.plugin.hudi.util.FileOperationUtils.FileOperation;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_MULTI_FG_PT_V8_MOR;
import static io.trino.plugin.hudi.util.FileOperationAssertions.assertAlluxioFileSystemAccesses;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.INDEX_DEFINITION;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.METADATA_TABLE;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.METADATA_TABLE_PROPERTIES;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.TABLE_PROPERTIES;

@ResourceLock("HUDI_CACHE_SYSTEM")
@Execution(ExecutionMode.SAME_THREAD)
public class TestHudiAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    private Path cacheDirectory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));

        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder()
                .put("hudi.metadata-enabled", "true")
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hudi.metadata.cache.enabled", "false")
                .buildOrThrow();

        return HudiQueryRunner.builder()
                .addConnectorProperties(hudiProperties)
                .setDataLoader(new ResourceHudiTablesInitializer())
                .setWorkerCount(0)
                .build();
    }

    @Test
    public void testSelectWithFilter()
            throws InterruptedException
    {
        @Language("SQL") String query = "SELECT * FROM " + HUDI_MULTI_FG_PT_V8_MOR + " WHERE country='SG'";
        Multiset<FileOperation> expectedFileOperations = ImmutableMultiset.<FileOperation>builder()
                .addCopies(new FileOperation("Alluxio.readCached", DATA), 2)
                .addCopies(new FileOperation("Alluxio.readCached", METADATA_TABLE), 20)
                .addCopies(new FileOperation("InputFile.lastModified", METADATA_TABLE), 5)
                .addCopies(new FileOperation("InputFile.length", METADATA_TABLE), 11)
                .addCopies(new FileOperation("InputFile.newStream", INDEX_DEFINITION), 2)
                .add(new FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES))
                .addCopies(new FileOperation("InputFile.newStream", TABLE_PROPERTIES), 2)
                .build();
        assertAlluxioFileSystemAccesses(getDistributedQueryRunner(), query, expectedFileOperations);

        assertAlluxioFileSystemAccesses(getDistributedQueryRunner(), query, expectedFileOperations);
    }

    @Test
    public void testJoin()
            throws InterruptedException
    {
        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts FROM " +
                HUDI_MULTI_FG_PT_V8_MOR + " t1 " +
                "INNER JOIN " + HUDI_MULTI_FG_PT_V8_MOR + " t2 ON t1.id = t2.id " +
                "WHERE t2.price <= 102";

        assertAlluxioFileSystemAccesses(
                getDistributedQueryRunner(),
                query,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Alluxio.readCached", DATA), 6)
                        .addCopies(new FileOperation("Alluxio.readCached", METADATA_TABLE), 222)
                        .addCopies(new FileOperation("InputFile.lastModified", METADATA_TABLE), 60)
                        .addCopies(new FileOperation("InputFile.length", METADATA_TABLE), 114)
                        .addCopies(new FileOperation("InputFile.newStream", INDEX_DEFINITION), 5)
                        .addCopies(new FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES), 3)
                        .addCopies(new FileOperation("InputFile.newStream", TABLE_PROPERTIES), 5)
                        .build());

        assertAlluxioFileSystemAccesses(
                getDistributedQueryRunner(),
                query,
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Alluxio.readCached", DATA), 6)
                        .addCopies(new FileOperation("Alluxio.readCached", METADATA_TABLE), 166)
                        .addCopies(new FileOperation("InputFile.lastModified", METADATA_TABLE), 45)
                        .addCopies(new FileOperation("InputFile.length", METADATA_TABLE), 85)
                        .addCopies(new FileOperation("InputFile.newStream", INDEX_DEFINITION), 4)
                        .addCopies(new FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES), 2)
                        .addCopies(new FileOperation("InputFile.newStream", TABLE_PROPERTIES), 4)
                        .build());
    }
}
