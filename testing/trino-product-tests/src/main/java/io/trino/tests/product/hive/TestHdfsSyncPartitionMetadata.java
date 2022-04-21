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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static java.lang.String.format;

public class TestHdfsSyncPartitionMetadata
        extends BaseTestSyncPartitionMetadata
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    private final String schema = "test_" + randomTableSuffix();

    @Override
    protected String schemaLocation()
    {
        return format("%s/%s", warehouseDirectory, schema);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Override
    public void testAddPartition()
    {
        super.testAddPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Override
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testAddPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Override
    public void testDropPartition()
    {
        super.testDropPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Override
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testDropPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Override
    public void testFullSyncPartition()
    {
        super.testFullSyncPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Override
    public void testInvalidSyncMode()
    {
        super.testInvalidSyncMode();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Override
    public void testMixedCasePartitionNames()
    {
        super.testMixedCasePartitionNames();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Override
    public void testConflictingMixedCasePartitionNames()
    {
        super.testConflictingMixedCasePartitionNames();
    }
}
