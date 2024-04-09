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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestInlineFunctions
{
    private final QueryAssertions assertions;

    public TestInlineFunctions()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testInlineFunction()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint) 
                    RETURNS bigint 
                    RETURN x * 2
                SELECT my_func(nationkey) 
                FROM nation 
                WHERE nationkey = 1
                """))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint) 
                    RETURNS bigint 
                    RETURN x * 2
                SELECT my_func(nationkey) 
                FROM nation 
                WHERE nationkey >= 1
                """))
                .matches("SELECT nationkey * 2 FROM nation WHERE nationkey >= 1");

        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint) 
                    RETURNS bigint 
                    RETURN x * 2
                SELECT my_func(nationkey) 
                FROM nation
                """))
                .matches("SELECT nationkey * 2 FROM nation");
    }
}
