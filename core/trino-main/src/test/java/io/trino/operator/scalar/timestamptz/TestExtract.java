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
package io.trino.operator.scalar.timestamptz;

import io.trino.operator.scalar.AbstractTestExtract;
import io.trino.sql.parser.ParsingException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExtract
        extends AbstractTestExtract
{
    @Override
    protected List<String> types()
    {
        return IntStream.rangeClosed(0, 12)
                .mapToObj(precision -> format("timestamp(%s) with time zone", precision))
                .collect(toImmutableList());
    }

    @Override
    public void testYear()
    {
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2020'");

        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2020'");
    }

    @Override
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '5'");

        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '5'");
    }

    @Override
    public void testWeek()
    {
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '19'");

        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '19'");
    }

    @Override
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '10'");

        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '10'");
    }

    @Override
    public void testDayOfMonth()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '10'");

        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '10'");
    }

    @Override
    public void testHour()
    {
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '12'");

        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '12'");
        assertThat(assertions.expression("hour(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '12'");
    }

    @Override
    public void testMinute()
    {
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '34'");

        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '34'");
        assertThat(assertions.expression("minute(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '34'");
    }

    @Override
    public void testSecond()
    {
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '56'");

        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '56'");
        assertThat(assertions.expression("second(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '56'");
    }

    @Test
    public void testMillisecond()
    {
        assertThatThrownBy(assertions.expression("EXTRACT(MILLISECOND FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")::evaluate)
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:12: Invalid EXTRACT field: MILLISECOND");

        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '0'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '100'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '120'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '123'");
        assertThat(assertions.expression("millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '123'");
    }

    @Override
    public void testTimezoneHour()
    {
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35')")).matches("BIGINT '8'");

        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35')")).matches("BIGINT '8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35')")).matches("BIGINT '8'");

        // negative offset
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35')")).matches("BIGINT '-8'");

        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35')")).matches("BIGINT '-8'");
        assertThat(assertions.expression("timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35')")).matches("BIGINT '-8'");
    }

    @Override
    public void testTimezoneMinute()
    {
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35')")).matches("BIGINT '35'");

        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35')")).matches("BIGINT '35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35')")).matches("BIGINT '35'");

        // negative offset
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35')")).matches("BIGINT '-35'");

        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35')")).matches("BIGINT '-35'");

        // negative minute offset
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35')")).matches("BIGINT '-35'");

        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35')")).matches("BIGINT '-35'");
        assertThat(assertions.expression("timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35')")).matches("BIGINT '-35'");
    }

    @Override
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '7'");

        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '7'");
    }

    @Override
    public void testDow()
    {
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '7'");
    }

    @Override
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '131'");

        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '131'");
    }

    @Override
    public void testDoy()
    {
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '131'");
    }

    @Override
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2'");

        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2'");
    }

    @Test
    public void testWeekOfYear()
    {
        assertThatThrownBy(assertions.expression("EXTRACT(WEEK_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")::evaluate)
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:12: Invalid EXTRACT field: WEEK_OF_YEAR");

        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '19'");
    }

    @Override
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2020'");

        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2020'");
    }

    @Override
    public void testYow()
    {
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu')")).matches("BIGINT '2020'");
    }
}
