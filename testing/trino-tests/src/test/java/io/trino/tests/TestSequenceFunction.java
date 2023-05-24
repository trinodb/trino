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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.operator.table.Sequence.SequenceFunctionSplit.DEFAULT_SPLIT_SIZE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSequenceFunction
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build()).build();
    }

    @Test
    public void testSequence()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(0, 8000, 3))
                """))
                .matches("SELECT * FROM UNNEST(sequence(0, 8000, 3))");

        assertThat(query("SELECT * FROM TABLE(sequence(1, 10, 3))"))
                .matches("VALUES BIGINT '1', 4, 7, 10");

        assertThat(query("SELECT * FROM TABLE(sequence(1, 10, 6))"))
                .matches("VALUES BIGINT '1', 7");

        assertThat(query("SELECT * FROM TABLE(sequence(-1, -10, -3))"))
                .matches("VALUES BIGINT '-1', -4, -7, -10");

        assertThat(query("SELECT * FROM TABLE(sequence(-1, -10, -6))"))
                .matches("VALUES BIGINT '-1', -7");

        assertThat(query("SELECT * FROM TABLE(sequence(-5, 5, 3))"))
                .matches("VALUES BIGINT '-5', -2, 1, 4");

        assertThat(query("SELECT * FROM TABLE(sequence(5, -5, -3))"))
                .matches("VALUES BIGINT '5', 2, -1, -4");

        assertThat(query("SELECT * FROM TABLE(sequence(0, 10, 3))"))
                .matches("VALUES BIGINT '0', 3, 6, 9");

        assertThat(query("SELECT * FROM TABLE(sequence(0, -10, -3))"))
                .matches("VALUES BIGINT '0', -3, -6, -9");
    }

    @Test
    public void testDefaultArguments()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(stop => 10))
                """))
                .matches("SELECT * FROM UNNEST(sequence(0, 10, 1))");
    }

    @Test
    public void testInvalidArgument()
    {
        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => -5,
                                    stop => 10,
                                    step => -2))
                """))
                .hasMessage("Step must be positive for sequence [-5, 10]");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => -5,
                                    step => 2))
                """))
                .hasMessage("Step must be negative for sequence [10, -5]");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => null,
                                    stop => -5,
                                    step => 2))
                """))
                .hasMessage("Start is null");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => null,
                                    step => 2))
                """))
                .hasMessage("Stop is null");

        assertThatThrownBy(() -> query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => -5,
                                    step => null))
                """))
                .hasMessage("Step is null");
    }

    @Test
    public void testSingletonSequence()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => 10,
                                    step => 2))
                """))
                .matches("VALUES BIGINT '10'");

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => 10,
                                    step => -2))
                """))
                .matches("VALUES BIGINT '10'");

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => 10,
                                    step => 0))
                """))
                .matches("VALUES BIGINT '10'");
    }

    @Test
    public void testBigStep()
    {
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => -5,
                                    step => %s))
                """.formatted(Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1))))
                .matches("VALUES BIGINT '10'");

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => -5,
                                    step => %s))
                """.formatted(Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1)))
                .matches("VALUES BIGINT '10'");

        assertThat(query("""
                SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x DESC)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1)))
                .matches(format("VALUES (null), (%s)", Long.MIN_VALUE / (DEFAULT_SPLIT_SIZE - 1) - 1));

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => 10,
                                    stop => -5,
                                    step => %s))
                """.formatted(Long.MIN_VALUE)))
                .matches("VALUES BIGINT '10'");

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => -5,
                                    stop => 10,
                                    step => %s))
                """.formatted(Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1))))
                .matches("VALUES BIGINT '-5'");

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => -5,
                                    stop => 10,
                                    step => %s))
                """.formatted(Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1)))
                .matches("VALUES BIGINT '-5'");

        assertThat(query("""
                SELECT DISTINCT x - lag(x, 1) OVER(ORDER BY x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1)))
                .matches(format("VALUES (null), (%s)", Long.MAX_VALUE / (DEFAULT_SPLIT_SIZE - 1) + 1));

        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => -5,
                                    stop => 10,
                                    step => %s))
                """.formatted(Long.MAX_VALUE)))
                .matches("VALUES BIGINT '-5'");
    }

    @Test
    public void testMultipleSplits()
    {
        long sequenceLength = DEFAULT_SPLIT_SIZE * 10 + DEFAULT_SPLIT_SIZE / 2;
        long start = 10;
        long step = 5;
        long stop = start + (sequenceLength - 1) * step;
        assertThat(query("""
                SELECT count(x), count(DISTINCT x), min(x), max(x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(start, stop, step)))
                .matches(format("SELECT BIGINT '%s', BIGINT '%s', BIGINT '%s', BIGINT '%s'", sequenceLength, sequenceLength, start, stop));

        sequenceLength = DEFAULT_SPLIT_SIZE * 4 + DEFAULT_SPLIT_SIZE / 2;
        stop = start + (sequenceLength - 1) * step;
        assertThat(query("""
                SELECT min(x), max(x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(start, stop, step)))
                .matches(format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));

        step = -5;
        stop = start + (sequenceLength - 1) * step;
        assertThat(query("""
                SELECT max(x), min(x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(start, stop, step)))
                .matches(format("SELECT BIGINT '%s', BIGINT '%s'", start, stop));
    }

    @Test
    public void testEdgeValues()
    {
        long start = Long.MIN_VALUE + 15;
        long stop = Long.MIN_VALUE + 3;
        long step = -10;
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s))
                """.formatted(start, stop, step)))
                .matches(format("VALUES (%s), (%s)", start, start + step));

        start = Long.MIN_VALUE + 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MIN_VALUE + 1;
        assertThat(query("""
                SELECT max(x), min(x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(start, stop, step)))
                .matches(format("SELECT %s, %s", start, Long.MIN_VALUE + 1));

        start = Long.MAX_VALUE - 15;
        stop = Long.MAX_VALUE - 3;
        step = 10;
        assertThat(query("""
                SELECT *
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s))
                """.formatted(start, stop, step)))
                .matches(format("VALUES (%s), (%s)", start, start + step));

        start = Long.MAX_VALUE - 1 - (DEFAULT_SPLIT_SIZE - 1) * step;
        stop = Long.MAX_VALUE - 1;
        assertThat(query("""
                SELECT min(x), max(x)
                FROM TABLE(sequence(
                                    start => %s,
                                    stop => %s,
                                    step => %s)) t(x)
                """.formatted(start, stop, step)))
                .matches(format("SELECT %s, %s", start, Long.MAX_VALUE - 1));
    }
}
