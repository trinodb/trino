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
package io.trino.plugin.kudu;

import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.testing.sql.TestTable.randomTableSuffix;

public abstract class AbstractKuduWithEmptyInferSchemaConnectorTest
        extends AbstractKuduConnectorTest
{
    @Override
    protected Optional<String> getKuduSchemaEmulationPrefix()
    {
        return Optional.of("");
    }

    @Test
    public void testListingOfTableForDefaultSchema()
    {
        assertQuery("SHOW TABLES FROM default", "VALUES '$schemas'");
    }

    @Test
    @Override
    public void testDropNonEmptySchema()
    {
        // Set column and table properties in CREATE TABLE statement
        String schemaName = "test_drop_non_empty_schema_" + randomTableSuffix();

        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("CREATE TABLE " + schemaName + ".t(x int WITH (primary_key=true)) WITH (partition_by_hash_columns=ARRAY['x'], partition_by_hash_buckets=2)");
            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + ".t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
