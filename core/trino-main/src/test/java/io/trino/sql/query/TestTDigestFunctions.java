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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.Math.round;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTDigestFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testValueAtQuantile()
    {
        assertThat(assertions.query(
                "SELECT value_at_quantile(tdigest_agg(d), 0.75e0) " +
                        "FROM (VALUES 0.1e0, 0.2e0, 0.3e0, 0.4e0) T(d)"))
                .matches("VALUES 0.4e0");

        assertThat(assertions.query(
                "SELECT value_at_quantile(tdigest_agg(d), 0.75e0) " +
                        "FROM (VALUES -0.1e0, -0.2e0, -0.3e0, -0.4e0) T(d)"))
                .matches("VALUES -0.1e0");

        assertThat(assertions.query(
                "SELECT value_at_quantile(tdigest_agg(d), 0.9e0) " +
                        "FROM (VALUES 0.1e0, 0.1e0, 0.1e0, 0.1e0, 10e0) T(d)"))
                .matches("VALUES 10e0");
    }

    @Test
    public void testValuesAtQuantiles()
    {
        assertThat(assertions.query(
                "SELECT values_at_quantiles(tdigest_agg(d), ARRAY[0.0001e0, 0.75e0, 0.85e0]) " +
                        "FROM (VALUES 0.1e0, 0.2e0, 0.3e0, 0.4e0) T(d)"))
                .matches("VALUES ARRAY[0.1e0, 0.4e0, 0.4e0]");

        assertThat(assertions.query(
                "SELECT values_at_quantiles(tdigest_agg(d), ARRAY[0.0001e0, 0.75e0, 0.85e0]) " +
                        "FROM (VALUES -0.1e0, -0.2e0, -0.3e0, -0.4e0) T(d)"))
                .matches("VALUES ARRAY[-0.4e0, -0.1e0, -0.10]");

        assertThat(assertions.query(
                "SELECT values_at_quantiles(tdigest_agg(d), ARRAY[0.0001e0, 0.75e0, 0.85e0]) " +
                        "FROM (VALUES 0.1e0, 0.1e0, 0.1e0, 0.1e0, 10e0) T(d)"))
                .matches("VALUES ARRAY[0.1e0, 0.1e0, 10.0e0]");

        assertThatThrownBy(() -> assertions.query(
                "SELECT values_at_quantiles(tdigest_agg(d), ARRAY[1e0, 0e0]) " +
                        "FROM (VALUES 0.1e0) T(d)"))
                .hasMessage("percentiles must be sorted in increasing order");
    }

    @Test
    public void testEmptyArrayOfQuantiles()
    {
        assertThat(assertions.query(
                "SELECT values_at_quantiles(tdigest_agg(d), ARRAY[]) " +
                        "FROM (VALUES 0.1e0, 0.2e0, 0.3e0, 0.4e0) T(d)"))
                .matches("VALUES CAST(ARRAY[] AS array(double))");
    }

    @Test
    public void testEmptyTDigestInput()
    {
        assertThat(assertions.query(
                "SELECT tdigest_agg(d) FROM (SELECT 1e0 WHERE false) T(d)"))
                .matches("VALUES CAST(null AS tdigest)");

        assertThat(assertions.query(
                "SELECT values_at_quantiles(qdigest_agg(d), ARRAY[0.5e0]) " +
                        "FROM (SELECT 1e0 WHERE false) T(d)"))
                .matches("VALUES CAST(null AS array(double))");
    }

    // Compare T-Digest vales_at_quantiles results to the results of Quantile Digest with very high accuracy.
    // Value set contains 2000 random numbers in range [0, 1000) with weights in range [1, 10)
    @Test
    public void testAccuracyAtHighAndLowPercentiles()
    {
        int size = 2000;
        Random random = new Random(1);
        long[] values = random.longs(size - 1, 0, 1000).toArray();
        long[] weights = random.longs(size - 1, 1, 10).toArray();
        StringBuilder builder = new StringBuilder("VALUES (BIGINT '1', BIGINT '1')");
        for (int i = 0; i < size - 1; i++) {
            builder.append(", (");
            builder.append(values[i]);
            builder.append(", ");
            builder.append(weights[i]);
            builder.append(")");
        }

        String percentilesArray = "ARRAY[0.00001, 0.0001, 0.001, 0.01, 0.99, 0.999, 0.9999, 0.99999]";

        MaterializedResult tDigestResult = assertions.getQueryRunner().execute(assertions.getDefaultSession(), "SELECT values_at_quantiles(tdigest_agg(n, w), " + percentilesArray + ") " +
                "FROM (" + builder.toString() + ") T(n, w)");

        MaterializedResult qDigestResult = assertions.getQueryRunner().execute(assertions.getDefaultSession(), "SELECT values_at_quantiles(qdigest_agg(n, w, 0.00001), " + percentilesArray + ") " +
                "FROM (" + builder.toString() + ") T(n, w)");

        List<Double> tDigestValuesAtPercentiles = (ArrayList<Double>) tDigestResult.getMaterializedRows().get(0).getField(0);
        List<Long> qDigestValuesAtPercentiles = (ArrayList<Long>) qDigestResult.getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < tDigestValuesAtPercentiles.size(); i++) {
            assertThat(qDigestValuesAtPercentiles.get(i).equals(round(tDigestValuesAtPercentiles.get(i))))
                    .isTrue();
        }
    }

    @Test
    public void testCastOperators()
    {
        assertThat(assertions.query(
                "SELECT values_at_quantiles(CAST(CAST(tdigest_agg(d) AS varbinary) AS tdigest), ARRAY[0, 1]) FROM (VALUES 1, 2, 3) T(d)"))
                .matches("VALUES CAST(ARRAY[1, 3] AS array(double))");
    }
}
