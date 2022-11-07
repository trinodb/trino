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

import io.trino.testing.BaseOrcWithBloomFiltersTest;
import io.trino.testing.QueryRunner;

import static java.lang.String.format;

public class TestIcebergOrcWithBloomFilters
        extends BaseOrcWithBloomFiltersTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("hive.orc.bloom-filters.enabled", "true")
                .addIcebergProperty("hive.orc.default-bloom-filter-fpp", "0.001")
                .build();
    }

    @Override
    protected String getTableProperties(String bloomFilterColumnName, String bucketingColumnName)
    {
        return format(
                "orc_bloom_filter_columns = ARRAY['%s'], partitioning = ARRAY['bucket(%s, 1)']",
                bloomFilterColumnName,
                bucketingColumnName);
    }
}
