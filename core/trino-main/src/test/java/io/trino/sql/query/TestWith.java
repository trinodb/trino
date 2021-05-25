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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWith
{
    private static final String CATALOG = "local";

    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner runner = LocalQueryRunner.builder(session)
                .build();

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testWithHiddenFields()
    {
        // Ensure WITH works when the subquery exposes hidden fields

        // First, verify the assumption that the nation table contains the expected hidden column
        assertThat(assertions.query(
                format(
                "SELECT count(*) " +
                        "FROM information_schema.columns " +
                        "WHERE table_catalog = '%s' and table_schema = '%s' and table_name = 'nation' and column_name = 'row_number'", CATALOG, TINY_SCHEMA_NAME)))
                .matches("VALUES BIGINT '0'");
        assertions.execute("SELECT min(row_number) FROM nation");

        assertThat(assertions.query(
                "WITH t(a, b, c, d) AS (TABLE nation) " +
                        "SELECT a, b FROM t WHERE a = 1"))
                .matches("VALUES (BIGINT '1', CAST('ARGENTINA' AS VARCHAR(25)))");
    }
}
