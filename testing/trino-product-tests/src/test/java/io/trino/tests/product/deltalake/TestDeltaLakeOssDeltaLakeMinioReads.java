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
class TestDeltaLakeOssDeltaLakeMinioReads
        extends BaseTestDeltaLakeMinioReadsJunit
{
    @Override
    protected String tableName()
    {
        return "region_deltalake";
    }

    @Override
    protected String regionResourcePath()
    {
        return "io/trino/plugin/deltalake/testing/resources/ossdeltalake/region";
    }

    @Override
    protected String expectedRegionParquetObjectName()
    {
        return tableName() + "/part-00000-274dcbf4-d64c-43ea-8eb7-e153feac98ce-c000.snappy.parquet";
    }
}
