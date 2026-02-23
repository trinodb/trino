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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for HiveKerberosKmsEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>All containers (KDC, KMS, Hadoop, Trino) start correctly</li>
 *   <li>Kerberos authentication is properly configured</li>
 *   <li>KMS is accessible and encryption zones work</li>
 *   <li>Trino can read/write encrypted data</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveKerberosKmsEnvironment.class)
class TestHiveKerberosKmsEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveKerberosKmsEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveKerberosKmsEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyKmsContainerRunning(HiveKerberosKmsEnvironment env)
    {
        assertThat(env.getKms().isRunning()).isTrue();
    }

    @Test
    void verifyEncryptionKeyExists(HiveKerberosKmsEnvironment env)
    {
        String keyName = env.getEncryptionKeyName();
        assertThat(keyName).isEqualTo("test_encryption_key");
    }

    @Test
    void verifyCreateTableInEncryptedZone(HiveKerberosKmsEnvironment env)
    {
        // Create a schema with its location in the encryption zone
        String schemaName = "encrypted_schema_" + randomNameSuffix();
        String tableName = "test_encrypted_" + randomNameSuffix();
        String encryptedPath = env.getEncryptionZonePath();
        String fullTableName = "hive." + schemaName + "." + tableName;

        try {
            // Create schema with location in encryption zone
            env.executeTrinoUpdate(
                    "CREATE SCHEMA hive." + schemaName +
                            " WITH (location = '" + encryptedPath + "/" + schemaName + "')");

            // Create a managed table (will be in the encrypted schema location)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (will be encrypted by HDFS)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (will be decrypted by HDFS)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void verifyCreateAndReadTableOutsideEncryptedZone(HiveKerberosKmsEnvironment env)
    {
        // Verify normal (non-encrypted) operations still work
        String tableName = "test_unencrypted_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (123)");

            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(123));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyMultipleRowsInEncryptedTable(HiveKerberosKmsEnvironment env)
    {
        // Create a schema with its location in the encryption zone
        String schemaName = "encrypted_multi_schema_" + randomNameSuffix();
        String tableName = "test_encrypted_multi_" + randomNameSuffix();
        String encryptedPath = env.getEncryptionZonePath();
        String fullTableName = "hive." + schemaName + "." + tableName;

        try {
            // Create schema with location in encryption zone
            env.executeTrinoUpdate(
                    "CREATE SCHEMA hive." + schemaName +
                            " WITH (location = '" + encryptedPath + "/" + schemaName + "')");

            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, name varchar)");

            env.executeTrinoUpdate(
                    "INSERT INTO " + fullTableName + " VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            assertThat(env.executeTrino("SELECT * FROM " + fullTableName + " ORDER BY id"))
                    .containsOnly(
                            row(1, "alice"),
                            row(2, "bob"),
                            row(3, "charlie"));

            assertThat(env.executeTrino("SELECT count(*) FROM " + fullTableName))
                    .containsOnly(row(3L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void verifyKerberosRealm(HiveKerberosKmsEnvironment env)
    {
        String realm = env.getKerberosRealm();
        assertThat(realm).isEqualTo("TRINO.TEST");
    }
}
