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

import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHudiFlociConnectorSmokeTest
        extends BaseHudiConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-hudi-connector-" + randomNameSuffix();
        Hive3FlociDataLake hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName, HIVE3_IMAGE));
        hiveFlociDataLake.start();

        return HudiQueryRunner.builder(hiveFlociDataLake)
                .addConnectorProperty("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                .setDataLoader(new TpchHudiTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }
}
