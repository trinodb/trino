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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_JDBC_URI;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_KEY;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_ROLE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_TEST_DATABASE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_USER;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergSnowflakeCatalogWithKeyPairConnectorSmokeTest
        extends TestIcebergSnowflakeCatalogConnectorSmokeTest
{
    @Override
    protected ImmutableMap<String, String> createIcebergProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", S3_ACCESS_KEY)
                .put("s3.aws-secret-key", S3_SECRET_KEY)
                .put("s3.region", S3_REGION)
                .put("iceberg.file-format", "parquet") // only Parquet is supported
                .put("iceberg.catalog.type", "snowflake")
                .put("iceberg.snowflake-catalog.role", SNOWFLAKE_ROLE)
                .put("iceberg.snowflake-catalog.database", SNOWFLAKE_TEST_DATABASE)
                .put("iceberg.snowflake-catalog.account-uri", SNOWFLAKE_JDBC_URI)
                .put("iceberg.snowflake-catalog.user", SNOWFLAKE_USER)
                .put("iceberg.snowflake-catalog.key", SNOWFLAKE_KEY)
                .buildOrThrow();
    }
}
