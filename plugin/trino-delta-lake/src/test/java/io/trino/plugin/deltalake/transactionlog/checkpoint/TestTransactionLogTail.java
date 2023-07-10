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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTransactionLogTail
{
    @Test(dataProvider = "dataSource")
    public void testTail(String dataSource)
            throws Exception
    {
        String tableLocation = getClass().getClassLoader().getResource(format("%s/person", dataSource)).toURI().toString();
        assertEquals(readJsonTransactionLogTails(tableLocation).size(), 7);
        assertEquals(updateJsonTransactionLogTails(tableLocation).size(), 7);
    }

    @DataProvider
    public Object[][] dataSource()
    {
        return new Object[][] {
                {"databricks"},
                {"deltalake"}
        };
    }

    private List<DeltaLakeTransactionLogEntry> updateJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, Optional.of(10L), Optional.of(12L));
        Optional<TransactionLogTail> updatedLogTail = transactionLogTail.getUpdatedTail(fileSystem, tableLocation);
        assertTrue(updatedLogTail.isPresent());
        return updatedLogTail.get().getFileEntries();
    }

    private List<DeltaLakeTransactionLogEntry> readJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tableLocation, Optional.of(10L));
        return transactionLogTail.getFileEntries();
    }
}
