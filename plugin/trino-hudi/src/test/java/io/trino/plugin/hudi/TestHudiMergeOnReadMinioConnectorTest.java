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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHudiMergeOnReadMinioConnectorTest
        extends BaseHudiMinioConnectorTest
{
    public TestHudiMergeOnReadMinioConnectorTest()
    {
        super(HudiTableType.MERGE_ON_READ);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-hudi-connector-" + randomNameSuffix();
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, HIVE3_IMAGE));
        hiveMinioDataLake.start();
        hiveMinioDataLake.getMinioClient().ensureBucketExists(bucketName);

        return S3HudiQueryRunner.create(
                ImmutableMap.of(),
                ImmutableMap.of("hudi.columns-to-hide", columnsToHide()),
                new TpchHudiTablesInitializer(HudiTableType.MERGE_ON_READ.getHoodieTableType(), REQUIRED_TPCH_TABLES),
                hiveMinioDataLake);
    }
}
