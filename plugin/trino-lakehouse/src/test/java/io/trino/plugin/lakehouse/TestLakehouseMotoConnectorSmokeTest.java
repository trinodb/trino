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
package io.trino.plugin.lakehouse;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.MotoContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.containers.MotoContainer.MOTO_ACCESS_KEY;
import static io.trino.testing.containers.MotoContainer.MOTO_REGION;
import static io.trino.testing.containers.MotoContainer.MOTO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLakehouseMotoConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MotoContainer moto = closeAfterClass(new MotoContainer());
        moto.start();
        moto.createBucket("test-bucket");

        return LakehouseQueryRunner.builder()
                .addLakehouseProperty("hive.metastore", "glue")
                .addLakehouseProperty("hive.metastore.glue.region", MOTO_REGION)
                .addLakehouseProperty("hive.metastore.glue.endpoint-url", moto.getEndpoint().toString())
                .addLakehouseProperty("hive.metastore.glue.aws-access-key", MOTO_ACCESS_KEY)
                .addLakehouseProperty("hive.metastore.glue.aws-secret-key", MOTO_SECRET_KEY)
                .addLakehouseProperty("hive.metastore.glue.default-warehouse-dir", "s3://test-bucket/")
                .addLakehouseProperty("fs.native-s3.enabled", "true")
                .addLakehouseProperty("s3.region", MOTO_REGION)
                .addLakehouseProperty("s3.endpoint", moto.getEndpoint().toString())
                .addLakehouseProperty("s3.aws-access-key", MOTO_ACCESS_KEY)
                .addLakehouseProperty("s3.aws-secret-key", MOTO_SECRET_KEY)
                .addLakehouseProperty("s3.path-style-access", "true")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        computeActual("CREATE SCHEMA lakehouse.tpch WITH (location='s3://test-bucket/tpch')");
        copyTpchTables(getQueryRunner(), "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = 's3://test-bucket/tpch/region-\\E.*\\Q',
                   type = 'ICEBERG'
                )\\E""");
    }
}
