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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestThriftMetastoreImpersonation
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake("test-thrift-impersonation-" + randomNameSuffix(), HIVE3_IMAGE));
        hiveMinioDataLake.start();
        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .setMetastoreImpersonationEnabled(true)
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.security", "allow-all")
                        .put("hive.metastore-cache-ttl", "1d")
                        .put("hive.user-metastore-cache-ttl", "1d")
                        .buildOrThrow())
                .build();
    }

    @Test
    void testFlushMetadataCache()
    {
        Session alice = Session.builder(getSession()).setIdentity(Identity.ofUser("alice")).build();

        try (TestTable table = newTrinoTable("test_partition", "(id int, part int) WITH (partitioned_by = ARRAY['part'])", List.of("1, 10"))) {
            assertThat(computeScalar(alice, "SELECT count(1) FROM \"" + table.getName() + "$partitions\""))
                    .isEqualTo(1L);

            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 20)", 1);
            assertThat(computeScalar(alice, "SELECT count(1) FROM \"" + table.getName() + "$partitions\""))
                    .isEqualTo(1L);

            assertUpdate(alice, "CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + table.getName() + "')");
            assertThat(computeScalar(alice, "SELECT count(1) FROM \"" + table.getName() + "$partitions\""))
                    .isEqualTo(2L);
        }
    }
}
