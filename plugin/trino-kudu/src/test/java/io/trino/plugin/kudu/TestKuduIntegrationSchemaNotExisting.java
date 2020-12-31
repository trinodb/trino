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
package io.prestosql.plugin.kudu;

import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestKuduIntegrationSchemaNotExisting
        extends AbstractTestQueryFramework
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String oldPrefix = System.getProperty("kudu.schema-emulation.prefix");
        System.setProperty("kudu.schema-emulation.prefix", "");
        try {
            kuduServer = new TestingKuduServer();
            return createKuduQueryRunner(kuduServer, "test_dummy");
        }
        catch (Throwable t) {
            System.setProperty("kudu.schema-emulation.prefix", oldPrefix);
            throw t;
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        kuduServer.close();
    }

    @Test
    public void testCreateTableWithoutSchema()
    {
        String createTable = "" +
                "CREATE TABLE IF NOT EXISTS kudu.test_presto_schema.test_presto_table (\n" +
                "id INT WITH (primary_key=true),\n" +
                "user_name VARCHAR\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2\n" +
                ")";

        try {
            assertUpdate(createTable);
            fail();
        }
        catch (Exception e) {
            assertEquals("Schema test_presto_schema not found", e.getMessage());
        }

        assertUpdate("CREATE SCHEMA kudu.test_presto_schema");
        assertUpdate(createTable);

        assertUpdate("DROP TABLE IF EXISTS kudu.test_presto_schema.test_presto_table");
        assertUpdate("DROP SCHEMA IF EXISTS kudu.test_presto_schema");
    }
}
