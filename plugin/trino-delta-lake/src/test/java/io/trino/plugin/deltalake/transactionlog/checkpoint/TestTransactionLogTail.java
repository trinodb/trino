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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTransactionLogTail
{
    @Test
    public void testTail()
            throws Exception
    {
        testTail("databricks73");
        testTail("deltalake");
    }

    private void testTail(String dataSource)
            throws Exception
    {
        String tableLocation = getClass().getClassLoader().getResource(format("%s/person", dataSource)).toURI().toString();
        assertThat(readJsonTransactionLogTails(tableLocation)).hasSize(7);
        assertThat(updateJsonTransactionLogTails(tableLocation)).hasSize(7);
    }

    private List<DeltaLakeTransactionLogEntry> updateJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, Optional.of(10L), Optional.of(12L), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        Optional<TransactionLogTail> updatedLogTail = transactionLogTail.getUpdatedTail(fileSystem, tableLocation, Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        assertThat(updatedLogTail).isPresent();
        return updatedLogTail.get().getFileEntries(fileSystem);
    }

    private List<DeltaLakeTransactionLogEntry> readJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, Optional.of(10L), Optional.empty(), DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE);
        return transactionLogTail.getFileEntries(fileSystem);
    }
}
