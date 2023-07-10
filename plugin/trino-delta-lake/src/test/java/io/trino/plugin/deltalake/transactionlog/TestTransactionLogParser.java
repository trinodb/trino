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

package io.trino.plugin.deltalake.transactionlog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import org.testng.annotations.Test;

import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getMandatoryCurrentVersion;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static org.testng.Assert.assertEquals;

public class TestTransactionLogParser
{
    @Test
    public void testGetCurrentVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);

        String basePath = getClass().getClassLoader().getResource("databricks").toURI().toString();

        assertEquals(getMandatoryCurrentVersion(fileSystem, appendPath(basePath, "simple_table_without_checkpoint")), 9);
        assertEquals(getMandatoryCurrentVersion(fileSystem, appendPath(basePath, "simple_table_ending_on_checkpoint")), 10);
        assertEquals(getMandatoryCurrentVersion(fileSystem, appendPath(basePath, "simple_table_past_checkpoint")), 11);
    }
}
