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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.sql.query.QueryAssertions;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTablesample
{
    private LocalQueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterClass(alwaysRun = true)
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
                .satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(50L, 450L));

        // 0.1%
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (1e-1)"))
                .satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(5L, 45L));

        // 0.1% as decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.1)"))
                .satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(5L, 45L));

        // fraction as long decimal
        assertThat(assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.000000000000000000001)"))
                .satisfies(result -> assertThat((Long) result.getOnlyValue()).isBetween(0L, 5L));
    }

    @Test
    public void testNullRatio()
    {
        // NULL
        assertPrestoExceptionThrownBy(() -> assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (NULL)"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL integer
        assertPrestoExceptionThrownBy(() -> assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS integer))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL double
        assertPrestoExceptionThrownBy(() -> assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS double))"))
                .hasErrorCode(INVALID_ARGUMENTS)
                .hasMessage("line 1:62: Sample percentage cannot be NULL");

        // NULL varchar
        assertPrestoExceptionThrownBy(() -> assertions.query("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (CAST(NULL AS varchar))"))
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:62: Sample percentage should be a numeric expression");
    }

    @Test
    public void testInvalidRatioType()
    {
        assertPrestoExceptionThrownBy(() -> assertions.query("SELECT count(*) FROM tpch.sf1.orders TABLESAMPLE BERNOULLI (DATE '1970-01-02')"))
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:61: Sample percentage should be a numeric expression");
    }
}
