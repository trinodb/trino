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
package io.trino.testing;

import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.testing.TestingMetadata.TestingTableHandle;

import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;

public final class TestingHandles
{
    private TestingHandles() {}

    private static final CatalogVersion TEST_CATALOG_VERSION = new CatalogVersion("test");
    public static final String TEST_CATALOG_NAME = "test-catalog";
    public static final CatalogHandle TEST_CATALOG_HANDLE = createTestCatalogHandle(TEST_CATALOG_NAME);
    public static final TableHandle TEST_TABLE_HANDLE = new TableHandle(
            TEST_CATALOG_HANDLE,
            new TestingTableHandle(),
            TestingTransactionHandle.create());

    public static CatalogHandle createTestCatalogHandle(String catalogName)
    {
        return createRootCatalogHandle(catalogName, TEST_CATALOG_VERSION);
    }
}
