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
package io.trino.plugin.deltalake;

import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.QueryRunner;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public abstract class BaseDeltaLakeAwsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    @Override
    protected HiveMinioDataLake createHiveMinioDataLake()
    {
        hiveMinioDataLake = new HiveMinioDataLake(bucketName);
        hiveMinioDataLake.start();
        return hiveMinioDataLake;
    }

    @Override
    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        hiveMinioDataLake.copyResources(resourcePath, table);
        queryRunner.execute(format(
                "CALL system.register_table('%s', '%s', '%s')",
                SCHEMA,
                table,
                getLocationForTable(bucketName, table)));
    }

    @Override
    protected String getLocationForTable(String bucketName, String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }

    @Override
    protected List<String> getTableFiles(String tableName)
    {
        return hiveMinioDataLake.listFiles(tableName).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    @Override
    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return hiveMinioDataLake.listFiles(transactionLogDirectory)
                .stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    @Override
    protected String bucketUrl()
    {
        return format("s3://%s/", bucketName);
    }
}
