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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigQueryAvroConnectorTest
        extends BaseBigQueryConnectorTest
{
    private static final Set<String> UNSUPPORTED_COLUMN_NAMES = ImmutableSet.<String>builder()
            .add("a-hyphen-minus")
            .add("a space")
            .add("atrailingspace ")
            .add(" aleadingspace")
            .add("a:colon")
            .add("an'apostrophe")
            .add("0startwithdigit")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("bigquery.job.label-name", "trino_query", "bigquery.job.label-format", "q_$QUERY_ID__t_$TRACE_TOKEN"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (UNSUPPORTED_COLUMN_NAMES.contains(columnName)) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    // TODO: Disable all operations for unsupported column names
    @Test(dataProvider = "unsupportedColumnNameDataProvider")
    public void testSelectFailsForColumnName(String columnName)
    {
        String tableName = "test.test_unsupported_column_name" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(\"" + columnName + "\" varchar(50))");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('test value')", 1);
            // The storage API can't read the table, but query based API can read it
            assertThatThrownBy(() -> query("SELECT * FROM " + tableName))
                    .cause()
                    .hasMessageMatching(".*(Illegal initial character|Invalid name).*");
            assertThat(bigQuerySqlExecutor.executeQuery("SELECT * FROM " + tableName).getValues())
                    .extracting(field -> field.get(0).getStringValue())
                    .containsExactly("test value");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] unsupportedColumnNameDataProvider()
    {
        return UNSUPPORTED_COLUMN_NAMES.stream().collect(toDataProvider());
    }
}
