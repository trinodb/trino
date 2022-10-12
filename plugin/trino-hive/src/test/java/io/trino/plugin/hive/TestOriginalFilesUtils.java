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
package io.trino.plugin.hive;

import com.google.common.io.Resources;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.orc.OriginalFilesUtils;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.hive.AcidInfo.OriginalFileInfo;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestOriginalFilesUtils
{
    private String tablePath;

    @BeforeClass
    public void setup()
            throws Exception
    {
        tablePath = new File(Resources.getResource(("dummy_id_data_orc")).toURI()).getPath();
    }

    @Test
    public void testGetPrecedingRowCountSingleFile()
    {
        List<OriginalFileInfo> originalFileInfoList = new ArrayList<>();
        originalFileInfoList.add(new OriginalFileInfo("000001_0", 730));

        long rowCountResult = OriginalFilesUtils.getPrecedingRowCount(
                originalFileInfoList,
                new Path(tablePath + "/000001_0"),
                HDFS_FILE_SYSTEM_FACTORY,
                SESSION.getIdentity(),
                new OrcReaderOptions(),
                new FileFormatDataSourceStats());
        assertEquals(rowCountResult, 0, "Original file should have 0 as the starting row count");
    }

    @Test
    public void testGetPrecedingRowCount()
    {
        List<OriginalFileInfo> originalFileInfos = new ArrayList<>();

        originalFileInfos.add(new OriginalFileInfo("000002_0", 741));
        originalFileInfos.add(new OriginalFileInfo("000002_0_copy_1", 768));
        originalFileInfos.add(new OriginalFileInfo("000002_0_copy_2", 743));

        long rowCountResult = OriginalFilesUtils.getPrecedingRowCount(
                originalFileInfos,
                new Path(tablePath + "/000002_0_copy_2"),
                HDFS_FILE_SYSTEM_FACTORY,
                SESSION.getIdentity(),
                new OrcReaderOptions(),
                new FileFormatDataSourceStats());
        // Bucket-2 has original files: 000002_0, 000002_0_copy_1. Each file original file has 4 rows.
        // So, starting row ID of 000002_0_copy_2 = row count of original files in Bucket-2 before it in lexicographic order.
        assertEquals(rowCountResult, 8, "Original file 000002_0_copy_2 should have 8 as the starting row count");
    }
}
