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
package io.trino.plugin.varada.warmup.model;

import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DateSlidingWindowWarmupPredicateRuleTest
{
    private static final String BASE_DATA = "2020-11-30";

    @Test
    public void testDateInWindow()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", BASE_DATA);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), "2020-11-25");
        assertThat(rule.test(partitionKeys)).isTrue();
    }

    @Test
    public void testDateBeforeWindow()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", BASE_DATA);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), "2020-11-10");
        assertThat(rule.test(partitionKeys)).isFalse();
    }

    @Test
    public void testDateBeforeWindowIncludeLastDate()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", BASE_DATA);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), "2020-11-20");
        assertThat(rule.test(partitionKeys)).isTrue();
    }

    @Test
    public void testNonExistPartitionColumnShouldReturnFalse()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", BASE_DATA);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("C2"), "2020-11-25");
        assertThat(rule.test(partitionKeys)).isFalse();
    }

    @Test
    public void testWrongFormat()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", BASE_DATA);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), "20201125");
        assertThat(rule.test(partitionKeys)).isFalse();
    }

    @Test
    public void testNullBaseDateShouldUseToday()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", null);
        Date todayMinus2Days = Date.from(Instant.now().minus(2, ChronoUnit.DAYS));
        LocalDate localDate = LocalDate.ofInstant(todayMinus2Days.toInstant(), ZoneId.systemDefault());
        String localDateStr = localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), localDateStr);
        assertThat(rule.test(partitionKeys)).isTrue();
    }

    @Test
    public void testEmptyBaseDateShouldUseToday()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", "");
        Date todayMinus2Days = Date.from(Instant.now().minus(2, ChronoUnit.DAYS));
        LocalDate localDate = LocalDate.ofInstant(todayMinus2Days.toInstant(), ZoneId.systemDefault());
        String localDateStr = localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), localDateStr);
        assertThat(rule.test(partitionKeys)).isTrue();
    }

    @Test
    public void testNullBaseDateShouldUseTodayOutOfRange()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", null);
        Date todayMinus12Days = Date.from(Instant.now().minus(12, ChronoUnit.DAYS));
        LocalDate localDate = LocalDate.ofInstant(todayMinus12Days.toInstant(), ZoneId.systemDefault());
        String localDateStr = localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), localDateStr);
        assertThat(rule.test(partitionKeys)).isFalse();
    }

    @Test
    public void testBaseDatePartOfRuleEquals()
    {
        DateSlidingWindowWarmupPredicateRule rule = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", null);
        DateSlidingWindowWarmupPredicateRule ruleWithBaseDate = new DateSlidingWindowWarmupPredicateRule("c1", 10, "yyyy-MM-dd", "2020-01-01");
        assertThat(rule.equals(ruleWithBaseDate)).isFalse();
    }
}
