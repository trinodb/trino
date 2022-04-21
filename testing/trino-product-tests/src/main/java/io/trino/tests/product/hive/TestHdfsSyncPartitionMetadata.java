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

import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static java.lang.String.format;

public class TestHdfsSyncPartitionMetadata
        extends AbstractTestSyncPartitionMetadata
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
}
