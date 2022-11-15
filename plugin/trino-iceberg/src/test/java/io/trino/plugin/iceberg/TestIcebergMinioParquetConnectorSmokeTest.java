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
package io.trino.plugin.iceberg;

import io.trino.filesystem.TrinoFileSystem;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileFormat.PARQUET;

public class TestIcebergMinioParquetConnectorSmokeTest
        extends BaseIcebergMinioConnectorSmokeTest
{
    public TestIcebergMinioParquetConnectorSmokeTest()
    {
        super(PARQUET);
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }
}
