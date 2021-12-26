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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestNumericalStability
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
    public void testVariance()
    {
        assertThat(assertions.query(
                "SELECT CAST(VAR_SAMP(x + exp(30))/VAR_SAMP(x) AS DECIMAL(3,2)) " +
                        "FROM (VALUES 1.0, 2.0, 3.0, 4.0, 5.0) AS X(x)"))
                .matches("VALUES 1.00");
    }

    @Test
    public void testCovariance()
    {
        assertThat(assertions.query(
                "SELECT CAST(COVAR_SAMP(x + exp(30), x + exp(30))/VAR_SAMP(x) AS DECIMAL(3,2)) " +
                        "FROM (VALUES 1.0, 2.0, 3.0, 4.0, 5.0) AS X(x)"))
                .matches("VALUES 1.00");
    }

    @Test
    public void testCorrelation()
    {
        assertThat(assertions.query(
                "SELECT CAST(CORR(x + exp(30), x + exp(30)) AS DECIMAL(3,2)) " +
                        "FROM (VALUES 1.0, 2.0, 3.0, 4.0, 5.0) AS X(x)"))
                .matches("VALUES 1.00");
    }

    @Test
    public void testRegressionSlope()
    {
        assertThat(assertions.query(
                "SELECT CAST(REGR_SLOPE((x + exp(30)) * 5 + 8, x + exp(30)) AS DECIMAL(3,2)) " +
                        "FROM (VALUES 1.0, 2.0, 3.0, 4.0, 5.0) AS X(x)"))
                .matches("VALUES 5.00");
    }

    @Test
    public void testRegressionIntercept()
    {
        assertThat(assertions.query(
                "SELECT CAST(REGR_INTERCEPT((x + exp(20)) * 5 + 8, x + exp(20)) AS DECIMAL(3,2)) " +
                        "FROM (VALUES 1.0, 2.0, 3.0, 4.0, 5.0) AS X(x)"))
                .matches("VALUES 8.00");
    }
}
