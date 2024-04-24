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
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

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
                "format = 'ORC', orc_bloom_filter_columns = ARRAY['%s'], partitioning = ARRAY['bucket(%s, 1)']",
                bloomFilterColumnName,
                bucketingColumnName);
    }

    @Test
    public void testBloomFilterPropertiesArePersistedDuringCreate()
    {
        String tableName = "test_metadata_write_properties_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + " (a bigint, b bigint, c bigint) WITH (" +
                "format = 'orc'," +
                "orc_bloom_filter_columns = array['a','b']," +
                "orc_bloom_filter_fpp = 0.1)");

        MaterializedResult actualProperties = computeActual("SELECT * FROM \"" + tableName + "$properties\"");
        assertThat(actualProperties).isNotNull();
        MaterializedResult expectedProperties = resultBuilder(getSession())
                .row("write.orc.bloom.filter.columns", "a,b")
                .row("write.orc.bloom.filter.fpp", "0.1").build();
        assertContains(actualProperties, expectedProperties);

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("orc_bloom_filter_columns", "orc_bloom_filter_fpp");
    }
}
