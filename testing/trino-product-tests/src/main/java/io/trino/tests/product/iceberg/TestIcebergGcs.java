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
package io.trino.tests.product.iceberg;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tests.product.BaseTestTableFormats;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.ICEBERG_GCS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;

public class TestIcebergGcs
        extends BaseTestTableFormats
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test(groups = {ICEBERG_GCS, PROFILE_SPECIFIC_TESTS})
    public void testCreateAndSelectNationTable()
    {
        super.testCreateAndSelectNationTable(warehouseDirectory);
    }

    @Test(groups = {ICEBERG_GCS, PROFILE_SPECIFIC_TESTS})
    public void testBasicWriteOperations()
    {
        super.testBasicWriteOperations(warehouseDirectory);
    }

    @Test(groups = {ICEBERG_GCS, PROFILE_SPECIFIC_TESTS})
    public void testPathContainsSpecialCharacter()
    {
        super.testPathContainsSpecialCharacter(warehouseDirectory, "partitioning");
    }

    @Test(groups = {ICEBERG_GCS, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingTrinoData()
    {
        super.testSparkCompatibilityOnTrinoCreatedTable(warehouseDirectory);
    }

    @Override
    protected String getCatalogName()
    {
        return "iceberg";
    }

    @Override
    protected String getSparkCatalog()
    {
        return "iceberg_test";
    }
}
