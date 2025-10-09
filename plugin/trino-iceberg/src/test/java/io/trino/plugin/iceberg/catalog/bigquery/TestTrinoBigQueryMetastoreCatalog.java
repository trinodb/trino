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
package io.trino.plugin.iceberg.catalog.bigquery;

import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import org.junit.jupiter.api.Disabled;

/**
 * Base catalog test for BigQuery Metastore.
 * This test is currently disabled because BigQueryMetastoreClientImpl is final and cannot be mocked easily.
 * The functionality is tested through the connector smoke test instead which uses real BigQuery infrastructure.
 *
 * To enable these tests, we would need to:
 * 1. Extract an interface from BigQueryMetastoreClientImpl
 * 2. Use that interface in the catalog implementation
 * 3. Create a test implementation of the interface
 */
@Disabled("BigQueryMetastoreClientImpl is final and cannot be mocked. Use TestIcebergBigQueryMetastoreCatalogConnectorSmokeTest instead.")
public class TestTrinoBigQueryMetastoreCatalog
        extends BaseTrinoCatalogTest
{
    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        throw new UnsupportedOperationException("This test is disabled. Use integration test instead.");
    }
}
