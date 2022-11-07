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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveFileBasedSecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        String path = new File(Resources.getResource(getClass(), "security.json").toURI()).getPath();
        queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.security", "file",
                        "security.config-file", path))
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testAdminCanRead()
    {
        Session admin = getSession("hive");
        queryRunner.execute(admin, "SELECT * FROM nation");
    }

    @Test
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("bob");
        assertThatThrownBy(() -> queryRunner.execute(bob, "SELECT * FROM nation"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching(".*Access Denied: Cannot select from table tpch.nation.*");
    }

    @Test
    public void testShowCreateSchemaDoesNotContainAuthorization()
    {
        Session admin = getSession("hive");
        assertThat((String) queryRunner.execute(admin, "SHOW CREATE SCHEMA tpch").getOnlyValue())
                .doesNotContain("AUTHORIZATION");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog())
                .setSchema(queryRunner.getDefaultSession().getSchema())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
