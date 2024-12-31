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
package io.trino.plugin.paimon;

import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.paimon.testing.TpchPaimonTablesInitializer;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestPaimonMinioConnectorSmokeTest
        extends BasePaimonConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-paimon-connector-" + randomNameSuffix();
        Hive3MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName, HIVE3_IMAGE));
        hiveMinioDataLake.start();
        hiveMinioDataLake.getMinioClient().ensureBucketExists(bucketName);

        return PaimonQueryRunner.builder(hiveMinioDataLake)
                .setDataLoader(new TpchPaimonTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }
}
