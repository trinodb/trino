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
package io.trino.plugin.hudi.util;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.trino.plugin.hudi.util.FileOperationUtils.FileOperation;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.Comparator;

import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getCacheOperationSpans;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.isTrinoSchemaOrPermissions;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

public final class FileOperationAssertions
{
    private static final Logger log = Logger.get(FileOperationAssertions.class);

    private FileOperationAssertions() {}

    /**
     * Asserts that file system accesses match expected operations.
     * This version uses manual filtering for Input/InputFile operations.
     * Logs detailed comparison at WARN level for debugging test failures.
     */
    public static void assertFileSystemAccesses(
            QueryRunner queryRunner,
            @Language("SQL") String query,
            Multiset<FileOperation> expectedCacheAccesses)
            throws InterruptedException
    {
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        // Allow time for table stats computation to finish before validation.
        Thread.sleep(1000L);
        Multiset<FileOperation> actualCacheAccesses = getFileOperations(queryRunner);
        printFileAccessDebugInfo(queryRunner, actualCacheAccesses, expectedCacheAccesses);
        assertMultisetsEqual(actualCacheAccesses, expectedCacheAccesses);
    }

    /**
     * Asserts that file system accesses match expected operations for Alluxio cache tests.
     * This version uses getCacheOperationSpans for filtering.
     * Logs detailed comparison at WARN level for debugging test failures.
     */
    public static void assertAlluxioFileSystemAccesses(
            QueryRunner queryRunner,
            @Language("SQL") String query,
            Multiset<FileOperation> expectedCacheAccesses)
            throws InterruptedException
    {
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        // Allow time for table stats computation to finish before validation.
        Thread.sleep(1000L);
        Multiset<FileOperation> actualCacheAccesses = getAlluxioFileOperations(queryRunner);
        printFileAccessDebugInfo(queryRunner, actualCacheAccesses, expectedCacheAccesses);
        assertMultisetsEqual(actualCacheAccesses, expectedCacheAccesses);
    }

    /**
     * Gets file operations from query runner spans using manual filtering.
     */
    public static Multiset<FileOperation> getFileOperations(QueryRunner queryRunner)
    {
        return queryRunner.getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("FileSystemCache."))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !span.getName().startsWith("InputFile.exists"))
                .filter(span -> !isTrinoSchemaOrPermissions(getFileLocation(span)))
                .map(FileOperation::create)
                .collect(toCollection(HashMultiset::create));
    }

    /**
     * Gets file operations for Alluxio cache tests using getCacheOperationSpans.
     */
    public static Multiset<FileOperation> getAlluxioFileOperations(QueryRunner queryRunner)
    {
        return getCacheOperationSpans(queryRunner)
                .stream()
                .filter(span -> !span.getName().startsWith("InputFile.exists"))
                .map(FileOperation::create)
                .collect(toCollection(HashMultiset::create));
    }

    private static void printFileAccessDebugInfo(
            QueryRunner queryRunner,
            Multiset<FileOperation> actualCacheAccesses,
            Multiset<FileOperation> expectedCacheAccesses)
    {
        // Log all file paths accessed for debugging
        log.warn("=== All File Paths Accessed ===");
        queryRunner.getSpans().stream()
                .filter(span -> span.getName().equals("InputFile.lastModified") || span.getName().equals("InputFile.length"))
                .forEach(span -> log.warn("%s: %s", span.getName(), getFileLocation(span)));

        // Log actual and expected cache accesses
        log.warn("=== Actual Cache Accesses ===");
        logSortedMultiset(actualCacheAccesses);

        log.warn("=== Expected Cache Accesses ===");
        logSortedMultiset(expectedCacheAccesses);

        // Calculate and log differences
        Multiset<FileOperation> extraInActual = HashMultiset.create(actualCacheAccesses);
        extraInActual.removeAll(expectedCacheAccesses);

        Multiset<FileOperation> missingFromActual = HashMultiset.create(expectedCacheAccesses);
        missingFromActual.removeAll(actualCacheAccesses);

        if (!extraInActual.isEmpty()) {
            log.warn("=== Extra in Actual (not expected) ===");
            logSortedMultiset(extraInActual);
        }

        if (!missingFromActual.isEmpty()) {
            log.warn("=== Missing from Actual (expected but not found) ===");
            logSortedMultiset(missingFromActual);
        }
    }

    private static void logSortedMultiset(Multiset<FileOperation> multiset)
    {
        multiset.entrySet().stream()
                .sorted(Comparator.comparing(a -> a.getElement().toString()))
                .forEach(entry -> log.warn("%s: %s", entry.getElement(), entry.getCount()));
    }
}
