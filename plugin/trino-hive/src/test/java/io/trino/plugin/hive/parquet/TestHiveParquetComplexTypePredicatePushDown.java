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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.BaseTestFileFormatComplexTypesPredicatePushDown;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveParquetComplexTypePredicatePushDown
        extends BaseTestFileFormatComplexTypesPredicatePushDown
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .addHiveProperty("hive.storage-format", "PARQUET")
                .build();
    }

    @Test
    public void ensureFormatParquet()
    {
        String tableName = "test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (colTest BIGINT)");
        assertThat(((String) computeScalar("SHOW CREATE TABLE " + tableName))).contains("PARQUET");
        assertUpdate("DROP TABLE " + tableName);
    }
}
