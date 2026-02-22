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

import io.trino.testing.containers.environment.RequiresEnvironment;

@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
class TestDeltaLakeDatabricksMinioReads
        extends BaseTestDeltaLakeMinioReadsJunit
{
    @Override
    protected String tableName()
    {
        return "region_databricks";
    }

    @Override
    protected String regionResourcePath()
    {
        return "io/trino/plugin/deltalake/testing/resources/databricks73/region";
    }

    @Override
    protected String expectedRegionParquetObjectName()
    {
        return tableName() + "/part-00000-98195ace-e492-4cd7-97d1-9b955202874b-c000.snappy.parquet";
    }
}
