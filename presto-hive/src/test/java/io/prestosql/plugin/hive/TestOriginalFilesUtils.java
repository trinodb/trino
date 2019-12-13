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
package io.prestosql.plugin.hive;

import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.hive.orc.OriginalFilesUtils;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static org.testng.Assert.assertTrue;

public class TestOriginalFilesUtils
{
    private static final String tablePath = Thread.currentThread().getContextClassLoader().getResource("dummy_id_data_orc").getPath();

    private static final Configuration config = new JobConf(new Configuration(false));

    private static final ConnectorSession SESSION = new TestingConnectorSession.Builder().build();

    @BeforeClass
    public void setup()
    {
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    @Test
    public void testGetRowCountSingleOriginalFileBucket()
    {
        List<OriginalFileLocations.OriginalFileInfo> originalFileInfos = new ArrayList<>();
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo("000001_0", 730));

        long rowCountResult = OriginalFilesUtils.getRowCount(originalFileInfos,
                new Path(tablePath + "/000001_0"),
                HDFS_ENVIRONMENT,
                SESSION.getUser(),
                new OrcReaderOptions(),
                config,
                new FileFormatDataSourceStats());
        assertTrue(rowCountResult == 0, "Original file should have 0 as the starting row count");
    }

    @Test
    public void testGetRowCountMultipleOriginalFilesBucket()
    {
        List<OriginalFileLocations.OriginalFileInfo> originalFileInfos = new ArrayList<>();

        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo("000002_0", 741));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo("000002_0_copy_1", 768));
        originalFileInfos.add(new OriginalFileLocations.OriginalFileInfo("000002_0_copy_2", 743));

        long rowCountResult = OriginalFilesUtils.getRowCount(originalFileInfos,
                new Path(tablePath + "/000002_0_copy_2"),
                HDFS_ENVIRONMENT,
                SESSION.getUser(),
                new OrcReaderOptions(),
                config,
                new FileFormatDataSourceStats());
        // Bucket-2 has original files: 000002_0, 000002_0_copy_1. Each file original file has 4 rows.
        // So, starting row ID of 000002_0_copy_2 = row count of original files in Bucket-2 before it in lexicographic order.
        assertTrue(rowCountResult == 8, "Original file 000002_0_copy_2 should have 8 as the starting row count");
    }
}
