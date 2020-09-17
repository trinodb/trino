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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.File;

import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestOrcDeleteDeltaPageSource
{
    @Test
    public void testReadingDeletedRows()
            throws Exception
    {
        File deleteDeltaFile = new File(Resources.getResource("fullacid_delete_delta_test/delete_delta_0000004_0000004_0000/bucket_00000").toURI());
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                new OrcReaderOptions(),
                "test",
                new JobConf(new Configuration(false)),
                HDFS_ENVIRONMENT,
                new FileFormatDataSourceStats());

        ConnectorPageSource pageSource = pageSourceFactory.createPageSource(new Path(deleteDeltaFile.toURI()), deleteDeltaFile.length()).orElseThrow();
        MaterializedResult materializedRows = MaterializedResult.materializeSourceDataStream(SESSION, pageSource, ImmutableList.of(BIGINT, BIGINT));

        assertEquals(materializedRows.getRowCount(), 1);
        assertEquals(materializedRows.getMaterializedRows().get(0), new MaterializedRow(5, 2L, 0L));
    }
}
