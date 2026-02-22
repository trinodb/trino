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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

@ProductTest
abstract class BaseTestDeltaLakeMinioReadsJunit
{
    protected abstract String tableName();

    protected abstract String regionResourcePath();

    @BeforeEach
    void setUp(DeltaLakeMinioEnvironment env)
    {
        MinioClient client = env.createMinioClient();
        client.copyResourcePath(env.getBucketName(), regionResourcePath(), tableName());
    }

    @Test
    @TestGroup.DeltaLakeMinio
    @TestGroup.ProfileSpecificTests
    void testReadRegionTable(DeltaLakeMinioEnvironment env)
    {
        try {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS delta.default.\"%s\"", tableName()));
            env.executeTrinoUpdate(format("CALL delta.system.register_table('default', '%1$s', 's3://%2$s/%1$s')", tableName(), env.getBucketName()));

            assertThat(env.executeTrino(format("SELECT count(name) FROM delta.default.\"%s\"", tableName())))
                    .containsOnly(row(5L));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS delta.default.\"%s\"", tableName()));
        }
    }
}
