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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public abstract class BaseDeltaLakeCompatibility
        extends AbstractTestQueryFramework
{
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

        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();
        try {
            String schemaName = queryRunner.getDefaultSession().getSchema().orElseThrow();
            TpchTable.getTables().forEach(table -> {
                String tableName = table.getTableName();
                hiveMinioDataLake.copyResources(resourcePath + tableName, schemaName + "/" + tableName);
                queryRunner.execute(format("CALL system.register_table(CURRENT_SCHEMA, '%2$s', 's3://%3$s/%1$s/%2$s')",
                        schemaName,
                        tableName,
                        bucketName));
            });
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testSelectAll()
    {
        for (TpchTable<?> table : TpchTable.getTables()) {
            String tableName = table.getTableName();
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck() // Delta Lake connector returns varchar, but TPCH connector returns varchar(n)
                    .matches("SELECT * FROM tpch.tiny." + tableName);
        }
    }
}
