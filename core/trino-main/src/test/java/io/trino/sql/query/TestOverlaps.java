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
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOverlaps
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
    public void testDateRangesOverlapping()
    {
        // [Jan, Jun] and [May, Dec] share May–Jun.
        assertThat(assertions.expression("(DATE '2020-01-01', DATE '2020-06-01') OVERLAPS (DATE '2020-05-01', DATE '2020-12-31')"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testDateRangesDisjoint()
    {
        assertThat(assertions.expression("(DATE '2020-01-01', DATE '2020-03-01') OVERLAPS (DATE '2020-05-01', DATE '2020-07-01')"))
                .matches("BOOLEAN 'false'");
    }

    @Test
    public void testHalfOpenEndExclusive()
    {
        // SQL OVERLAPS uses half-open semantics: period 1's end equals period 2's start ⇒ no overlap.
        assertThat(assertions.expression("(DATE '2020-01-01', DATE '2020-05-01') OVERLAPS (DATE '2020-05-01', DATE '2020-07-01')"))
                .matches("BOOLEAN 'false'");
    }

    @Test
    public void testSameStartsAlwaysOverlap()
    {
        // Per SQL spec, equal start points always overlap, even at a single instant.
        assertThat(assertions.expression("(DATE '2020-05-01', DATE '2020-05-01') OVERLAPS (DATE '2020-05-01', DATE '2020-05-01')"))
                .matches("BOOLEAN 'true'");
        assertThat(assertions.expression("(DATE '2020-05-01', DATE '2020-05-01') OVERLAPS (DATE '2020-05-01', DATE '2020-12-31')"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testReversedEndpoints()
    {
        // (end, start) is normalized to (start, end) before evaluation.
        assertThat(assertions.expression("(DATE '2020-06-01', DATE '2020-01-01') OVERLAPS (DATE '2020-05-01', DATE '2020-12-31')"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testIntervalEnd()
    {
        // The second column can be an interval; end is start + interval.
        assertThat(assertions.expression("(DATE '2020-01-01', INTERVAL '5' MONTH) OVERLAPS (DATE '2020-05-01', INTERVAL '7' MONTH)"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testTimestampWithSubsecond()
    {
        assertThat(assertions.expression(
                "(TIMESTAMP '2020-05-01 12:00:00.000', TIMESTAMP '2020-05-01 13:00:00.000') OVERLAPS " +
                        "(TIMESTAMP '2020-05-01 12:30:00.000', TIMESTAMP '2020-05-01 14:00:00.000')"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testMixedPrecisionCoerces()
    {
        // TIMESTAMP(0) and TIMESTAMP(3) coerce to common TIMESTAMP(3) and compare.
        assertThat(assertions.expression(
                "(TIMESTAMP '2020-05-01 12:00:00', TIMESTAMP '2020-05-01 13:00:00') OVERLAPS " +
                        "(TIMESTAMP '2020-05-01 12:30:00.123', TIMESTAMP '2020-05-01 14:00:00.456')"))
                .matches("BOOLEAN 'true'");
    }

    @Test
    public void testNullPropagation()
    {
        assertThat(assertions.expression("(CAST(NULL AS DATE), DATE '2020-06-01') OVERLAPS (DATE '2020-05-01', DATE '2020-12-31')"))
                .matches("CAST(NULL AS BOOLEAN)");
    }

    @Test
    public void testWrongRowDegreeRejected()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression(
                "(DATE '2020-01-01', DATE '2020-06-01', DATE '2020-12-31') OVERLAPS (DATE '2020-05-01', DATE '2020-07-01')").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("OVERLAPS operand must be a row of two elements");
    }

    @Test
    public void testWrongStartTypeRejected()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression(
                "(1, 2) OVERLAPS (3, 4)").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("OVERLAPS period start must be a datetime");
    }

    @Test
    public void testWrongEndTypeRejected()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression(
                "(DATE '2020-01-01', VARCHAR 'not a date') OVERLAPS (DATE '2020-05-01', DATE '2020-07-01')").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("OVERLAPS period end must be a datetime or interval");
    }

    @Test
    public void testIncompatibleShapesRejected()
    {
        // datetime-datetime vs datetime-interval cannot share a common row type.
        assertTrinoExceptionThrownBy(() -> assertions.expression(
                "(DATE '2020-01-01', DATE '2020-06-01') OVERLAPS (DATE '2020-05-01', INTERVAL '7' MONTH)").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("Cannot apply OVERLAPS");
    }
}
