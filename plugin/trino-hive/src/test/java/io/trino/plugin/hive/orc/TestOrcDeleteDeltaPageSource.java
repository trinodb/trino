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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.junit.jupiter.api.Test;

import java.io.File;

import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcDeleteDeltaPageSource
{
    @Test
    public void testReadingDeletedRows()
            throws Exception
    {
        File deleteDeltaFile = new File(Resources.getResource("fullacid_delete_delta_test/delete_delta_0000004_0000004_0000/bucket_00000").toURI());
        TrinoInputFile inputFile = new LocalInputFile(deleteDeltaFile);
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                new OrcReaderOptions(),
                new FileFormatDataSourceStats());

        ConnectorPageSource pageSource = pageSourceFactory.createPageSource(inputFile).orElseThrow();
        MaterializedResult materializedRows = MaterializedResult.materializeSourceDataStream(SESSION, pageSource, ImmutableList.of(BIGINT, INTEGER, BIGINT));

        assertThat(materializedRows.getRowCount()).isEqualTo(1);

        AcidOutputFormat.Options bucketOptions = new AcidOutputFormat.Options(new Configuration(false)).bucket(0);
        assertThat(materializedRows.getMaterializedRows().get(0)).isEqualTo(new MaterializedRow(5, 2L, BucketCodec.V1.encode(bucketOptions), 0L));
    }
}
