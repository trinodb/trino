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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;

final class TestLakehouseFileBasedSecurity
        extends AbstractTestQueryFramework
{
    private static final List<String> TABLES = ImmutableList.of("hive_nation", "iceberg_nation", "delta_nation");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String securityConfigFile = new File(Resources.getResource(getClass(), "security.json").toURI()).getPath();
        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .amendSession(session -> session.setIdentity(Identity.ofUser("admin")))
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", createTempDirectory("lakehouse_file_based_security").toUri().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .addLakehouseProperty("hive.security", "file")
                .addLakehouseProperty("security.config-file", securityConfigFile)
                .build();
        queryRunner.execute("CREATE SCHEMA lakehouse.tpch");
        queryRunner.execute("CREATE TABLE hive_nation WITH (type = 'HIVE') AS SELECT * FROM tpch.tiny.nation");
        queryRunner.execute("CREATE TABLE iceberg_nation WITH (type = 'ICEBERG') AS SELECT * FROM tpch.tiny.nation");
        queryRunner.execute("CREATE TABLE delta_nation WITH (type = 'DELTA') AS SELECT * FROM tpch.tiny.nation");
        return queryRunner;
    }

    @Test
    void testSelect()
    {
        for (String table : TABLES) {
            assertQuerySucceeds("SELECT * FROM " + table);
            assertQuerySucceeds(sessionFor("alice"), "SELECT * FROM " + table);
            assertAccessDenied(sessionFor("bob"), "SELECT * FROM " + table, "Cannot select from table tpch." + table);
        }
    }

    @Test
    void testInsert()
    {
        for (String table : TABLES) {
            assertAccessDenied(sessionFor("alice"), "INSERT INTO " + table + " SELECT * FROM " + table, "Cannot insert into table tpch." + table);
        }
    }

    @Test
    void testSelectFromSystemTable()
    {
        // $properties is checked against the rules of its source table
        for (String table : TABLES) {
            assertQuerySucceeds(sessionFor("alice"), "SELECT * FROM \"" + table + "$properties\"");
            assertAccessDenied(sessionFor("bob"), "SELECT * FROM \"" + table + "$properties\"", "Cannot select from table tpch." + table + "\\$properties");
        }
    }

    private Session sessionFor(String user)
    {
        return testSessionBuilder()
                .setCatalog(getSession().getCatalog())
                .setSchema(getSession().getSchema())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
