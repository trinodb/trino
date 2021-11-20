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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveSystemSecurity
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of("hive.security", "system"))
                .setInitialTables(ImmutableList.of())
                .build();
    }

    @Test
    public void testDefinerView()
    {
        assertUpdate("CREATE VIEW system_security_definer_view AS SELECT 42 as n");
        // select from view fails because the view does not have an owner in Hive and there is no system security system to provide the run-as identity.
        assertQueryFails("SELECT * from system_security_definer_view", "Catalog does not support run-as DEFINER views: hive.tpch.system_security_definer_view");
        assertUpdate("DROP VIEW system_security_definer_view");
    }

    @Test
    public void testInvokerView()
    {
        assertUpdate("CREATE VIEW system_security_invoker_view SECURITY INVOKER AS SELECT 42 as n");
        assertQuery("SELECT * from system_security_invoker_view", "SELECT 42 as n");
        assertQueryFails("ALTER VIEW system_security_invoker_view SET AUTHORIZATION user", "Catalog does not support permission management: hive");
        // disabled system security metadata does not have any roles
        assertQueryFails("ALTER VIEW system_security_invoker_view SET AUTHORIZATION ROLE PUBLIC", ".*Role 'public' does not exist");
        assertUpdate("DROP VIEW system_security_invoker_view");
    }

    @Test
    public void testCreateSchema()
    {
        assertUpdate("CREATE SCHEMA system_security_schema");
        assertThat((String) computeScalar("SHOW CREATE SCHEMA system_security_schema"))
                .doesNotContain("AUTHORIZATION");
        assertQueryFails("ALTER SCHEMA system_security_schema SET AUTHORIZATION user", "Catalog does not support permission management: hive");
        // disabled system security metadata does not have any roles
        assertQueryFails("ALTER SCHEMA system_security_schema SET AUTHORIZATION ROLE PUBLIC", ".*Role 'public' does not exist");
        assertUpdate("DROP SCHEMA system_security_schema");
    }

    @Test
    public void testTableSchema()
    {
        assertUpdate("CREATE table system_security_table (n bigint)");
        assertQueryFails("ALTER TABLE system_security_table SET AUTHORIZATION user", "Catalog does not support permission management: hive");
        // disabled system security metadata does not have any roles
        assertQueryFails("ALTER TABLE system_security_table SET AUTHORIZATION ROLE PUBLIC", ".*Role 'public' does not exist");
        assertUpdate("DROP TABLE system_security_table");
    }
}
