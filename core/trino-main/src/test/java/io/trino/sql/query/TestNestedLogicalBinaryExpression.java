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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Regression test for https://github.com/trinodb/trino/issues/9250
 */
@TestInstance(PER_CLASS)
public class TestNestedLogicalBinaryExpression
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner runner = LocalQueryRunner.builder(session)
                .build();

        runner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(1), ImmutableMap.of());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void test()
    {
        assertThat(assertions.query("SELECT orderkey FROM orders WHERE custkey IS NULL OR custkey = 370 AND orderkey = 1"))
                .matches("VALUES BIGINT '1'");
    }
}
