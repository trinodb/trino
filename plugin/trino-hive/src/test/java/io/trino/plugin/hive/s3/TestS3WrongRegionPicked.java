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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3WrongRegionPicked
{
    @Test
    public void testS3WrongRegionSelection()
            throws Exception
    {
        // Bucket names are global so a unique one needs to be used.
        String bucketName = "test-bucket" + randomNameSuffix();

        try (HiveMinioDataLake dataLake = new HiveMinioDataLake(bucketName)) {
            dataLake.start();
            try (QueryRunner queryRunner = S3HiveQueryRunner.builder(dataLake)
                    .setHiveProperties(ImmutableMap.of("hive.s3.region", "eu-central-1")) // Different than the default one
                    .build()) {
                String tableName = "s3_region_test_" + randomNameSuffix();
                queryRunner.execute("CREATE TABLE default." + tableName + " (a int) WITH (external_location = 's3://" + bucketName + "/" + tableName + "')");
                assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM default." + tableName))
                        .rootCause()
                        .hasMessageContaining("Status Code: 400")
                        .hasMessageContaining("Error Code: AuthorizationHeaderMalformed"); // That is how Minio reacts to bad region
            }
        }
    }
}
