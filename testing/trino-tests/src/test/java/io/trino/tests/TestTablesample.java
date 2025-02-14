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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTablesample
{
    private QueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        Closeables.closeAll(queryRunner, assertions);
        queryRunner = null;
        assertions = null;
    }

    @Test
    public void testTablesample()
    {
        // zero sample
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0)"))
                .matches("VALUES BIGINT '0'");

        // full sample
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (100)"))
                .matches("VALUES BIGINT '15000'");

        // 1%
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (1)"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(50L, 450L));

        // 0.1%
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (1e-1)"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(3L, 45L));

        // 0.1% as decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.1)"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(3L, 45L));

        // fraction as long decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.000000000000000000001)"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(0L, 5L));
    }

    @Test
    public void testNullRatio()
    {
        // NULL
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (NULL)"))
                .failure()
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL integer
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS integer))"))
                .failure()
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL double
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS double))"))
                .failure()
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL varchar
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS varchar))"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:62: Sample percentage should be a numeric expression");
    }

    @Test
    public void testInvalidRatioType()
    {
        assertThat(assertions.query("SELECT count(*) FROM tpch.sf1.orders TABLESAMPLE BERNOULLI (DATE '1970-01-02')"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:61: Sample percentage should be a numeric expression");
    }

    @Test
    public void testInSubquery()
    {
        // zero sample
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0))"))
                .matches("VALUES BIGINT '0'");

        // full sample
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (100))"))
                .matches("VALUES BIGINT '15000'");

        // 1%
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (1))"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(50L, 450L));

        // 0.1%
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (1e-1))"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(3L, 45L));

        // 0.1% as decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.1))"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(3L, 45L));

        // fraction as long decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.000000000000000000001))"))
                .result().satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(0L, 5L));
    }
}
