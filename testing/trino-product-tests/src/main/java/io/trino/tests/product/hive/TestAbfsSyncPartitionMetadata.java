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

import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.ABFS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestAbfsSyncPartitionMetadata
        extends AbstractTestSyncPartitionMetadata
{
    private final String schema = "test_" + randomTableSuffix();

    @Override
    protected String schemaLocation()
    {
        String container = requireNonNull(System.getenv("ABFS_CONTAINER"), "Environment variable not set: ABFS_CONTAINER");
        String account = requireNonNull(System.getenv("ABFS_ACCOUNT"), "Environment variable not set: ABFS_ACCOUNT");
        return format("abfs://%s@%s.dfs.core.windows.net/%s", container, account, schema);
    }

    @Test(groups = ABFS)
    @Override
    public void tearDown()
    {
        super.tearDown();
    }

    @Test(groups = ABFS)
    @Override
    public void testAddPartition()
    {
        super.testAddPartition();
    }

    @Test(groups = ABFS)
    @Override
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testAddPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = ABFS)
    @Override
    public void testDropPartition()
    {
        super.testDropPartition();
    }

    @Test(groups = ABFS)
    @Override
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testDropPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = ABFS)
    @Override
    public void testFullSyncPartition()
    {
        super.testFullSyncPartition();
    }

    @Test(groups = ABFS)
    @Override
    public void testInvalidSyncMode()
    {
        super.testInvalidSyncMode();
    }

    @Test(groups = ABFS)
    @Override
    public void testMixedCasePartitionNames()
    {
        super.testMixedCasePartitionNames();
    }

    @Test(groups = ABFS)
    @Override
    public void testConflictingMixedCasePartitionNames()
    {
        super.testConflictingMixedCasePartitionNames();
    }
}
