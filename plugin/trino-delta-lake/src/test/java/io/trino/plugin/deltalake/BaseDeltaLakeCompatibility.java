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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDeltaLakeCompatibility
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_schema";

    protected final String bucketName;
    protected final String resourcePath;
    protected HiveMinioDataLake hiveMinioDataLake;

    public BaseDeltaLakeCompatibility(String resourcePath)
    {
        this.bucketName = "compatibility-test-queries-" + randomNameSuffix();
        this.resourcePath = requireNonNull(resourcePath);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        QueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.register-table-procedure.enabled", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
        TpchTable.getTables().forEach(table -> {
            String tableName = table.getTableName();
            hiveMinioDataLake.copyResources(resourcePath + tableName, SCHEMA + "/" + tableName);
            queryRunner.execute(format("CALL system.register_table('%1$s', '%2$s', 's3://%3$s/%1$s/%2$s')",
                    SCHEMA,
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @Test(dataProvider = "tpchTablesDataProvider")
    public void testSelectAll(String tableName)
    {
        assertThat(query("SELECT * FROM " + tableName))
                .skippingTypesCheck() // Delta Lake connector returns varchar, but TPCH connector returns varchar(n)
                .matches("SELECT * FROM tpch.tiny." + tableName);
    }

    @DataProvider
    public static Object[][] tpchTablesDataProvider()
    {
        return TpchTable.getTables().stream()
                .map(table -> new Object[] {table.getTableName()})
                .toArray(Object[][]::new);
    }
}
