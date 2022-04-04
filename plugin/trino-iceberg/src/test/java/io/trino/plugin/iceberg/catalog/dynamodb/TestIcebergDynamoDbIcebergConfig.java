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
package io.trino.plugin.iceberg.catalog.dynamodb;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergDynamoDbIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DynamoDbIcebergConfig.class)
                .setCatalogName(null)
                .setTableName("iceberg")
                .setDefaultWarehouseDir(null)
                .setConnectionUrl(null)
                .setAccessKey(null)
                .setSecretKey(null)
                .setRegion(null)
                .setIamRole(null)
                .setExternalId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.dynamodb.catalog-name", "test-catalog")
                .put("iceberg.dynamodb.table-name", "test-table")
                .put("iceberg.dynamodb.default-warehouse-dir", "/dev/null")
                .put("iceberg.dynamodb.connection-url", "http://localhost:8000")
                .put("iceberg.dynamodb.aws.access-key", "access")
                .put("iceberg.dynamodb.aws.secret-key", "secret")
                .put("iceberg.dynamodb.aws.region", "region")
                .put("iceberg.dynamodb.aws.iam-role", "iamRole")
                .put("iceberg.dynamodb.external-id", "externalId")
                .buildOrThrow();

        DynamoDbIcebergConfig expected = new DynamoDbIcebergConfig()
                .setCatalogName("test-catalog")
                .setTableName("test-table")
                .setDefaultWarehouseDir("/dev/null")
                .setConnectionUrl("http://localhost:8000")
                .setAccessKey("access")
                .setSecretKey("secret")
                .setRegion("region")
                .setIamRole("iamRole")
                .setExternalId("externalId");

        assertFullMapping(properties, expected);
    }
}
