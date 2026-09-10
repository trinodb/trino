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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTablePrivilegesPreservation
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder().build();
        metastore = getHiveMetastore(queryRunner);
        return queryRunner;
    }

    @Test
    public void testPrivilegesPreservedOnInsert()
    {
        String tableName = "test_privileges_preserved_" + randomNameSuffix();
        String schema = getSession().getSchema().orElseThrow();

        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");

        try {
            String tableOwner = metastore.getTable(schema, tableName).orElseThrow().getOwner().orElseThrow();
            HivePrincipal grantee = new HivePrincipal(USER, "test_user");
            HivePrincipal grantor = new HivePrincipal(USER, tableOwner);
            metastore.grantTablePrivileges(schema, tableName, tableOwner, grantee, grantor, ImmutableSet.of(SELECT), false);

            Set<HivePrivilegeInfo> privilegesBefore = metastore.listTablePrivileges(schema, tableName, Optional.of(tableOwner), Optional.of(grantee));
            assertThat(privilegesBefore)
                    .extracting(HivePrivilegeInfo::getHivePrivilege)
                    .containsExactly(SELECT);

            // INSERT triggers commitToExistingTable which calls replaceTable
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

            Set<HivePrivilegeInfo> privilegesAfter = metastore.listTablePrivileges(schema, tableName, Optional.of(tableOwner), Optional.of(grantee));
            assertThat(privilegesAfter)
                    .extracting(HivePrivilegeInfo::getHivePrivilege)
                    .containsExactly(SELECT);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
