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
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.hudi.common.model.HoodieTableType;
import org.testng.annotations.Test;

import static io.trino.plugin.hudi.BaseHudiConnectorTest.columnsToHide;
import static io.trino.plugin.hudi.S3HudiQueryRunner.HUDI_MINIO_TESTS;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHudiMinioConnectorTest
        extends BaseConnectorTest
{
    private final HoodieTableType tableType;
    protected HiveMinioDataLake hiveMinioDataLake;

    public BaseHudiMinioConnectorTest(HoodieTableType tableType)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-hudi-connector-" + randomTableSuffix();
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        hiveMinioDataLake.getMinioClient().ensureBucketExists(bucketName);

        return S3HudiQueryRunner.create(
                HUDI_MINIO_TESTS.getCatalogName(),
                HUDI_MINIO_TESTS.getSchemaName(),
                ImmutableMap.<String, String>builder()
                        .put("hudi.columns-to-hide", columnsToHide())
                        .buildOrThrow(),
                new TpchHudiTablesInitializer(tableType, REQUIRED_TPCH_TABLES),
                hiveMinioDataLake);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_INSERT:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/orders'\n\\Q" +
                        ")");
    }
}
