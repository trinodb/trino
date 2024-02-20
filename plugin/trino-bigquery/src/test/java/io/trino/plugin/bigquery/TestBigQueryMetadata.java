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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestBigQueryMetadata
{
    @Test
    public void testDatasetNotFoundMessage()
    {
        // Update the catch block condition in 'BigQueryMetadata.listTables()' method if this test failed
        BigQuery bigQuery = new BigQuerySqlExecutor().getBigQuery();
        assertThatExceptionOfType(BigQueryException.class)
                .isThrownBy(() -> bigQuery.listTables("test_dataset_not_found"))
                .matches(e -> e.getCode() == 404 && e.getMessage().contains("Not found: Dataset"));
    }
}
