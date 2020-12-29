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
package io.trino.operator.scalar.date;

import io.trino.operator.scalar.AbstractTestExtract;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExtract
        extends AbstractTestExtract
{
    @Override
    protected List<String> types()
    {
        return List.of("date");
    }

    @Override
    public void testYear()
    {
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("year(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Override
    public void testMonth()
    {
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("EXTRACT(MONTH FROM DATE '1960-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(DATE '2020-05-10')")).matches("BIGINT '5'");
        assertThat(assertions.expression("month(DATE '1960-05-10')")).matches("BIGINT '5'");
    }

    @Override
    public void testWeek()
    {
        assertThat(assertions.expression("EXTRACT(WEEK FROM DATE '2020-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("EXTRACT(WEEK FROM DATE '1960-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(DATE '2020-05-10')")).matches("BIGINT '19'");
        assertThat(assertions.expression("week(DATE '1960-05-10')")).matches("BIGINT '19'");
    }

    @Override
    public void testDay()
    {
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY FROM DATE '1960-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day(DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Override
    public void testDayOfMonth()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_MONTH FROM DATE '1960-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(DATE '2020-05-10')")).matches("BIGINT '10'");
        assertThat(assertions.expression("day_of_month(DATE '1960-05-10')")).matches("BIGINT '10'");
    }

    @Override
    public void testDayOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("day_of_week(DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("day_of_week(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Override
    public void testDow()
    {
        assertThat(assertions.expression("EXTRACT(DOW FROM DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("EXTRACT(DOW FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("dow(DATE '2020-05-10')")).matches("BIGINT '7'");
        assertThat(assertions.expression("dow(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Override
    public void testDayOfYear()
    {
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("day_of_year(DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Override
    public void testDoy()
    {
        assertThat(assertions.expression("EXTRACT(DOY FROM DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("EXTRACT(DOY FROM DATE '1960-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("doy(DATE '2020-05-10')")).matches("BIGINT '131'");
        assertThat(assertions.expression("doy(DATE '1960-05-10')")).matches("BIGINT '131'");
    }

    @Override
    public void testQuarter()
    {
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("EXTRACT(QUARTER FROM DATE '1960-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(DATE '2020-05-10')")).matches("BIGINT '2'");
        assertThat(assertions.expression("quarter(DATE '1960-05-10')")).matches("BIGINT '2'");
    }

    @Override
    public void testYearOfWeek()
    {
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("year_of_week(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("year_of_week(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }

    @Override
    public void testYow()
    {
        assertThat(assertions.expression("EXTRACT(YOW FROM DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("EXTRACT(YOW FROM DATE '1960-05-10')")).matches("BIGINT '1960'");
        assertThat(assertions.expression("yow(DATE '2020-05-10')")).matches("BIGINT '2020'");
        assertThat(assertions.expression("yow(DATE '1960-05-10')")).matches("BIGINT '1960'");
    }
}
