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
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * End-to-end validation that keystore-resolved S3 credentials work for Hive DML against MinIO.
 */
@ProductTest
@RequiresEnvironment(S3KeystoreSecretsEnvironment.class)
@TestGroup.S3KeystoreSecrets
@TestGroup.ProfileSpecificTests
class TestS3KeystoreSecretsSqlTests
{
    private static final String SCHEMA = "hive.test_keystore";
    private static final String TABLE = SCHEMA + ".orders";
    private static final String TABLE_LOCATION = S3KeystoreSecretsEnvironment.SCHEMA_LOCATION + "orders/";

    @Test
    void testKeystoreBackedS3ReadWrite(S3KeystoreSecretsEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + TABLE);
        env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + SCHEMA);
        env.executeTrinoUpdate("CREATE SCHEMA " + SCHEMA);
        env.executeTrinoUpdate("CREATE TABLE " + TABLE + " (id bigint, name varchar) WITH (external_location = '" + TABLE_LOCATION + "', format = 'PARQUET')");
        env.executeTrinoUpdate("INSERT INTO " + TABLE + " VALUES (1, 'keystore-ok'), (2, 'minio-rw')");

        QueryResult result = env.executeTrino("SELECT id, name FROM " + TABLE + " ORDER BY id");
        assertThat(result).containsOnly(
                row(1L, "keystore-ok"),
                row(2L, "minio-rw"));
    }
}
