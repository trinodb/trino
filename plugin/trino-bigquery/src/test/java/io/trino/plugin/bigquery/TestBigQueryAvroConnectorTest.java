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
import io.trino.Session;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

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
            .add("カラム")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.arrow-serialization.enabled", "false")
                        .put("bigquery.job.label-name", "trino_query")
                        .put("bigquery.job.label-format", "q_$QUERY_ID__t_$TRACE_TOKEN")
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
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
    @Test
    public void testSelectFailsForColumnName()
    {
        for (String columnName : UNSUPPORTED_COLUMN_NAMES) {
            String tableName = "test.test_unsupported_column_name" + randomNameSuffix();

            assertUpdate("CREATE TABLE " + tableName + "(\"" + columnName + "\" varchar(50))");
            try {
                assertUpdate("INSERT INTO " + tableName + " VALUES ('test value')", 1);
                // The storage API can't read the table, but query based API can read it
                assertThat(query("SELECT * FROM " + tableName))
                        .failure().hasMessageMatching("(Cannot create read|Invalid Avro schema).*(Illegal initial character|Invalid name).*");
                assertThat(bigQuerySqlExecutor.executeQuery("SELECT * FROM " + tableName).getValues())
                        .extracting(field -> field.getFirst().getStringValue())
                        .containsExactly("test value");
            }
            finally {
                assertUpdate("DROP TABLE " + tableName);
            }
        }
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        // Bigquery does not have small file threshold properties
        return session;
    }
}
