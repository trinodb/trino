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

import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTransactionLogTail
{
    @Test(dataProvider = "dataSource")
    public void testTail(String dataSource)
            throws Exception
    {
        String tableLocation = format("%s/person", dataSource);
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
        URI resource = getClass().getClassLoader().getResource(tableLocation).toURI();
        Path tablePath = new Path(resource);
        Configuration config = new Configuration(false);
        FileSystem fileSystem = tablePath.getFileSystem(config);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(fileSystem, tablePath, Optional.of(10L), Optional.of(12L));
        Optional<TransactionLogTail> updatedLogTail = transactionLogTail.getUpdatedTail(fileSystem, tablePath);
        assertTrue(updatedLogTail.isPresent());
        return updatedLogTail.get().getFileEntries();
    }

    private List<DeltaLakeTransactionLogEntry> readJsonTransactionLogTails(String tableLocation)
            throws Exception
    {
        URI resource = getClass().getClassLoader().getResource(tableLocation).toURI();
        Path tablePath = new Path(resource);
        Configuration config = new Configuration(false);
        FileSystem filesystem = tablePath.getFileSystem(config);
        TransactionLogTail transactionLogTail = TransactionLogTail.loadNewTail(filesystem, tablePath, Optional.of(10L));
        return transactionLogTail.getFileEntries();
    }
}
