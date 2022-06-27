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
package io.trino.tests.product.deltalake;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.google.common.base.Throwables;
import org.testng.Assert.ThrowingRunnable;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TransactionLogAssertions
{
    private TransactionLogAssertions() {}

    public static void assertLastEntryIsCheckpointed(AmazonS3 s3Client, String bucketName, String tableName)
    {
        Optional<String> lastJsonEntry = listJsonLogEntries(s3Client, bucketName, tableName).stream().max(String::compareTo);
        assertThat(lastJsonEntry).isPresent();
        Optional<String> lastCheckpointEntry = listCheckpointEntries(s3Client, bucketName, tableName).stream().max(String::compareTo);
        assertThat(lastCheckpointEntry).isPresent();

        assertEquals(lastJsonEntry.get().replace(".json", ""), lastCheckpointEntry.get().replace(".checkpoint.parquet", ""));
    }

    public static void assertTransactionLogVersion(AmazonS3 s3Client, String bucketName, String tableName, int versionNumber)
    {
        Optional<String> lastJsonEntry = listJsonLogEntries(s3Client, bucketName, tableName).stream().max(String::compareTo);
        assertThat(lastJsonEntry).isPresent();
        assertEquals(lastJsonEntry.get(), format("%020d.json", versionNumber));
    }

    public static void assertNewVersion(AmazonS3 s3Client, String bucketName, String tableName, ThrowingRunnable runnable)
    {
        long initialVersion = getTransactionLogVersion(s3Client, bucketName, tableName);
        try {
            runnable.run();
        }
        catch (Throwable e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        assertThat(getTransactionLogVersion(s3Client, bucketName, tableName))
                .isGreaterThan(initialVersion);
    }

    public static void assertNoNewVersion(AmazonS3 s3Client, String bucketName, String tableName, ThrowingRunnable runnable)
    {
        long initialVersion = getTransactionLogVersion(s3Client, bucketName, tableName);
        try {
            runnable.run();
        }
        catch (Throwable e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        assertThat(getTransactionLogVersion(s3Client, bucketName, tableName))
                .isEqualTo(initialVersion);
    }

    private static long getTransactionLogVersion(AmazonS3 s3Client, String bucketName, String tableName)
    {
        Optional<String> lastJsonEntry = listJsonLogEntries(s3Client, bucketName, tableName).stream().max(String::compareTo);
        if (lastJsonEntry.isEmpty()) {
            fail("Cannot determine version for table " + tableName);
        }
        return parseLong(lastJsonEntry.get().split("\\.")[0]);
    }

    private static List<String> listJsonLogEntries(AmazonS3 s3Client, String bucketName, String tableName)
    {
        return listLogEntries(s3Client, bucketName, tableName, file -> file.endsWith(".json"));
    }

    private static List<String> listCheckpointEntries(AmazonS3 s3Client, String bucketName, String tableName)
    {
        return listLogEntries(s3Client, bucketName, tableName, file -> file.endsWith(".checkpoint.parquet"));
    }

    private static List<String> listLogEntries(AmazonS3 s3Client, String bucketName, String tableName, Predicate<String> fileFilter)
    {
        String prefix = "databricks-compatibility-test-" + tableName + "/_delta_log/";
        ListObjectsV2Result listResult = s3Client.listObjectsV2(bucketName, prefix);
        return listResult.getObjectSummaries().stream()
                .map(s3Object -> {
                    String[] path = s3Object.getKey().split("/");
                    return path[path.length - 1];
                })
                .filter(fileFilter)
                .collect(toImmutableList());
    }
}
