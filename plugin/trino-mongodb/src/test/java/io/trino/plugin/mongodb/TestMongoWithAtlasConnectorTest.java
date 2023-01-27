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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.mongodb.MongoAtlasQueryRunner.createMongoAtlasQueryRunner;
import static io.trino.plugin.mongodb.TestingMongoAtlasInfoProvider.getConnectionString;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMongoWithAtlasConnectorTest
        extends TestMongoConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        client = closeAfterClass(MongoClients.create(getConnectionString()));
        return createMongoAtlasQueryRunner(getConnectionString(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Invalid namespace specified");
    }

    // Overridden as max length of the table name is different from the one returned by TestMongoConnectorTest#maxTableNameLength and
    // the message is different from the return value of TestMongoConnectorTest#verifyTableNameLengthFailurePermissible
    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        String baseTableName = "test_create_" + randomNameSuffix();
        int maxLength = 255 - "tpch.".length(); // Length is different from value returned by TestMongoConnectorTest#maxTableNameLength
        String validTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
        assertUpdate("DROP TABLE " + validTableName);

        String invalidTableName = validTableName + "z";
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + invalidTableName + " (a bigint)"))
                .satisfies((e) -> assertThat(e).hasMessageMatching(".*Fully qualified namespace is too long.*")); // Message is different compared to TestMongoConnectorTest#verifyTableNameLengthFailurePermissible
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isFalse();
    }

    // Overridden to create fewer tables
    @Test
    @Override
    public void testListTablesFromSchemaWithBigAmountOfTables()
    {
        String schemaName = "huge_schema_" + randomNameSuffix();
        int totalTables = 50;
        MongoDatabase database = client.getDatabase(schemaName);
        for (int i = 0; i < totalTables; i++) {
            database.createCollection("table_" + i);
        }

        assertThat(getQueryRunner().execute("SHOW TABLES FROM mongodb." + schemaName).getRowCount()).isEqualTo(totalTables);
        for (int i = 0; i < totalTables; i++) {
            database.getCollection("table_" + i).drop();
        }
        // Do not have to drop schema as empty schema will be automatically deleted
    }
}
