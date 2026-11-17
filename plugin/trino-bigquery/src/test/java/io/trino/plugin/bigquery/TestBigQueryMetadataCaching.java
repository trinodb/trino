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
import com.google.cloud.bigquery.DatasetId;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static com.google.cloud.bigquery.BigQuery.DatasetDeleteOption.deleteContents;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryMetadataCaching
        extends AbstractTestQueryFramework
{
    private BigQuery bigQuery;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociBigQuery flociBigQuery = closeAfterClass(new FlociBigQuery());
        flociBigQuery.start();
        bigQuery = flociBigQuery.bigQuery();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("bigquery")
                        .setSchema("tpch")
                        .build())
                .build();
        queryRunner.installPlugin(new TestingBigQueryPlugin(flociBigQuery.endpoint()));
        queryRunner.createCatalog("bigquery", "bigquery", ImmutableMap.<String, String>builder()
                .put("bigquery.credentials-key", flociBigQuery.credentialsKey())
                .put("bigquery.metadata.cache-ttl", "5m").buildOrThrow());
        return queryRunner;
    }

    @Test
    public void testMetadataCaching()
    {
        String schema = "test_metadata_caching_" + randomNameSuffix();
        try {
            getQueryRunner().execute("CREATE SCHEMA " + schema);
            assertThat(getQueryRunner().execute("SHOW SCHEMAS IN bigquery LIKE '" + schema + "'").getOnlyValue()).isEqualTo(schema);

            String schemaTableName = schema + ".test_metadata_caching";
            getQueryRunner().execute("CREATE TABLE " + schemaTableName + " AS SELECT 1 x");
            assertThat(getQueryRunner().execute("SELECT * FROM " + schemaTableName).getRowCount()).isEqualTo(1);

            bigQuery.delete(DatasetId.of(schema), deleteContents());
            assertThat(getQueryRunner().execute("SHOW SCHEMAS IN bigquery LIKE '" + schema + "'").getOnlyValue()).isEqualTo(schema);

            assertQueryFails("SELECT * FROM " + schemaTableName, ".*Schema '.+' does not exist.*");
        }
        finally {
            bigQuery.delete(DatasetId.of(schema), deleteContents());
        }
    }
}
