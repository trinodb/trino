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

import io.trino.filesystem.Location;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.orc.OriginalFilesUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.hive.AcidInfo.OriginalFileInfo;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestOriginalFilesUtils
{
    private Location tablePath;

    @BeforeClass
    public void setup()
            throws Exception
    {
        tablePath = Location.of(getResource("dummy_id_data_orc").toString());
    }

    @Test
    public void testGetPrecedingRowCountSingleFile()
    {
        List<OriginalFileInfo> originalFileInfoList = new ArrayList<>();
        originalFileInfoList.add(new OriginalFileInfo("000001_0", 730));

        long rowCountResult = OriginalFilesUtils.getPrecedingRowCount(
                originalFileInfoList,
                tablePath.appendPath("000001_0"),
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
                tablePath.appendPath("000002_0_copy_2"),
                HDFS_FILE_SYSTEM_FACTORY,
                SESSION.getIdentity(),
                new OrcReaderOptions(),
                new FileFormatDataSourceStats());
        // Bucket-2 has original files: 000002_0, 000002_0_copy_1. Each file original file has 4 rows.
        // So, starting row ID of 000002_0_copy_2 = row count of original files in Bucket-2 before it in lexicographic order.
        assertEquals(rowCountResult, 8, "Original file 000002_0_copy_2 should have 8 as the starting row count");
    }
}
