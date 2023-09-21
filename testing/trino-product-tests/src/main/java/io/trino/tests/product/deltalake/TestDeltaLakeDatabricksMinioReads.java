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

import org.testng.annotations.Test;

import static io.minio.messages.EventType.OBJECT_ACCESSED_GET;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_MINIO;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.MinioNotificationsAssertions.assertNotificationsCount;

public class TestDeltaLakeDatabricksMinioReads
        extends BaseTestDeltaLakeMinioReads
{
    public TestDeltaLakeDatabricksMinioReads()
    {
        super("region_databricks", "io/trino/plugin/deltalake/testing/resources/databricks73/region");
    }

    @Override
    @Test(groups = {DELTA_LAKE_MINIO, PROFILE_SPECIFIC_TESTS})
    public void testReadRegionTable()
    {
        super.testReadRegionTable();
        assertNotificationsCount(NOTIFICATIONS_TABLE, OBJECT_ACCESSED_GET, tableName + "/part-00000-98195ace-e492-4cd7-97d1-9b955202874b-c000.snappy.parquet", 2);
    }
}
