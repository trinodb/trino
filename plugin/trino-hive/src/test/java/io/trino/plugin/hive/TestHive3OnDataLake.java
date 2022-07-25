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

import io.trino.plugin.hive.containers.HiveHadoop;
import org.testng.annotations.Test;

public class TestHive3OnDataLake
        extends BaseTestHiveOnDataLake
{
    public TestHive3OnDataLake()
    {
        super(HiveHadoop.HIVE3_IMAGE);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedAcidTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3",
                "format = 'ORC'",
                "transactional = true"));
        assertInsertFailure(
                testTable,
                "Overwriting existing partition in transactional tables doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
    }
}
