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
import java.util.function.Supplier;

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
        // Async table-stats computation can outlive the synchronous query and emit spans into
        // the exporter after execute returns. A fixed Thread.sleep races with this: when stats
        // from query N is still running while query N+1's measurement happens, spans leak
        // across the boundary and counts get scrambled. Poll until the span set is stable for
        // two consecutive reads.
        Multiset<FileOperation> actualCacheAccesses = waitForStableSpans(() -> getFileOperations(queryRunner));
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
        // See assertFileSystemAccesses for the rationale behind polling instead of a fixed sleep.
        Multiset<FileOperation> actualCacheAccesses = waitForStableSpans(() -> getAlluxioFileOperations(queryRunner));
        printFileAccessDebugInfo(queryRunner, actualCacheAccesses, expectedCacheAccesses);
        assertMultisetsEqual(actualCacheAccesses, expectedCacheAccesses);
    }

    /**
     * Returns the span set once two consecutive reads (200ms apart) agree. Bounded by a
     * 30-second ceiling so a runaway test fails loudly instead of hanging.
     */
    private static Multiset<FileOperation> waitForStableSpans(Supplier<Multiset<FileOperation>> reader)
            throws InterruptedException
    {
        long deadlineMillis = System.currentTimeMillis() + 30_000L;
        Multiset<FileOperation> previous = null;
        while (System.currentTimeMillis() < deadlineMillis) {
            Thread.sleep(200L);
            Multiset<FileOperation> current = reader.get();
            if (previous != null && current.equals(previous)) {
                return current;
            }
            previous = current;
        }
        return previous != null ? previous : reader.get();
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
