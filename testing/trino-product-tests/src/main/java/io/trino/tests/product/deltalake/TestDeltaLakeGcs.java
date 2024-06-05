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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tests.product.BaseTestTableFormats;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.DELTA_LAKE_GCS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;

public class TestDeltaLakeGcs
        extends BaseTestTableFormats
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test(groups = {DELTA_LAKE_GCS, PROFILE_SPECIFIC_TESTS})
    public void testCreateAndSelectNationTable()
    {
        super.testCreateAndSelectNationTable(warehouseDirectory);
    }

    @Test(groups = {DELTA_LAKE_GCS, PROFILE_SPECIFIC_TESTS})
    public void testBasicWriteOperations()
    {
        super.testBasicWriteOperations(warehouseDirectory);
    }

    @Test(groups = {DELTA_LAKE_GCS, PROFILE_SPECIFIC_TESTS})
    public void testPathContainsSpecialCharacter()
    {
        super.testPathContainsSpecialCharacter(warehouseDirectory, "partitioned_by");
    }

    @Override
    protected String getCatalogName()
    {
        return "delta";
    }
}
