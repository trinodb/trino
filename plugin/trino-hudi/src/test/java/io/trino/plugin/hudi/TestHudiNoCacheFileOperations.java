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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.plugin.hudi.util.FileOperationUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Map;

import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.isTrinoSchemaOrPermissions;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_MULTI_FG_PT_V8_MOR;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.INDEX_DEFINITION;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.LOG;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.METADATA_TABLE;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.METADATA_TABLE_PROPERTIES;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.TABLE_PROPERTIES;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.TIMELINE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

@ResourceLock("HUDI_CACHE_SYSTEM")
@Execution(ExecutionMode.SAME_THREAD)
public class TestHudiNoCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder()
                .put("hudi.metadata-enabled", "true")
                .put("hudi.metadata.cache.enabled", "false")
                .put("fs.cache.enabled", "false")
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
        assertFileSystemAccesses(
                query,
                ImmutableMultiset.<FileOperationUtils.FileOperation>builder()
                        .addCopies(new FileOperationUtils.FileOperation("Input.readTail", DATA), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.lastModified", METADATA_TABLE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.length", METADATA_TABLE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE), 6)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", INDEX_DEFINITION), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TIMELINE), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", LOG), 1)
                        .add(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES))
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TABLE_PROPERTIES), 2)
                        .build());

        assertFileSystemAccesses(
                query,
                ImmutableMultiset.<FileOperationUtils.FileOperation>builder()
                        .addCopies(new FileOperationUtils.FileOperation("Input.readTail", DATA), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.lastModified", METADATA_TABLE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.length", METADATA_TABLE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", INDEX_DEFINITION), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE), 6)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TIMELINE), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", LOG), 1)
                        .add(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES))
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TABLE_PROPERTIES), 2)
                        .build());
    }

    @Test
    public void testJoin()
            throws InterruptedException
    {
        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts FROM " +
                HUDI_MULTI_FG_PT_V8_MOR + " t1 " +
                "INNER JOIN " + HUDI_MULTI_FG_PT_V8_MOR + " t2 ON t1.id = t2.id " +
                "WHERE t2.price <= 102";

        assertFileSystemAccesses(query,
                ImmutableMultiset.<FileOperationUtils.FileOperation>builder()
                        .addCopies(new FileOperationUtils.FileOperation("Input.readTail", DATA), 6)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.lastModified", METADATA_TABLE), 39)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.length", METADATA_TABLE), 39)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", INDEX_DEFINITION), 5)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE), 54)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES), 3)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TABLE_PROPERTIES), 5)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TIMELINE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", LOG), 2)
                        .build());

        assertFileSystemAccesses(query,
                ImmutableMultiset.<FileOperationUtils.FileOperation>builder()
                        .addCopies(new FileOperationUtils.FileOperation("Input.readTail", DATA), 6)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.lastModified", METADATA_TABLE), 29)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.length", METADATA_TABLE), 29)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", INDEX_DEFINITION), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE), 40)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", METADATA_TABLE_PROPERTIES), 2)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TABLE_PROPERTIES), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", TIMELINE), 4)
                        .addCopies(new FileOperationUtils.FileOperation("InputFile.newStream", LOG), 2)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperationUtils.FileOperation> expectedCacheAccesses)
               throws InterruptedException
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        // Allow time for table stats computation to finish before validation.
        Thread.sleep(1000L);
        assertMultisetsEqual(getFileOperations(queryRunner), expectedCacheAccesses);
    }

    private static Multiset<FileOperationUtils.FileOperation> getFileOperations(QueryRunner queryRunner)
    {
        return queryRunner.getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("FileSystemCache."))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !span.getName().startsWith("InputFile.exists"))
                .filter(span -> !isTrinoSchemaOrPermissions(getFileLocation(span)))
                .map(FileOperationUtils.FileOperation::create)
                .collect(toCollection(HashMultiset::create));
    }
}
