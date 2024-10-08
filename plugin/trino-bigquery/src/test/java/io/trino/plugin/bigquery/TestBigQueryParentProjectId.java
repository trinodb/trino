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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

class TestBigQueryParentProjectId
        extends AbstractTestQueryFramework
{
    private final String testingProjectId;
    private final String testingParentProjectId;

    TestBigQueryParentProjectId()
    {
        testingProjectId = requiredNonEmptySystemProperty("testing.bigquery-project-id");
        testingParentProjectId = requiredNonEmptySystemProperty("testing.bigquery-parent-project-id");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.project-id", testingProjectId)
                        .put("bigquery.parent-project-id", testingParentProjectId)
                        .buildOrThrow())
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    void testQueriesWithParentProjectId()
    {
        assertThat(computeScalar("SELECT name FROM bigquery.tpch.nation WHERE nationkey = 0")).isEqualTo("ALGERIA");
        assertThat(computeScalar("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM tpch.nation WHERE nationkey = 0'))")).isEqualTo("ALGERIA");
        assertThat(computeScalar(format("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM %s.tpch.nation WHERE nationkey = 0'))", testingProjectId)))
                .isEqualTo("ALGERIA");
    }
}
