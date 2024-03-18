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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static io.trino.plugin.varada.warmup.model.DateRangeSlidingWindowWarmupPredicateRule.UNLIMITED_START_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

public class DateRangeSlidingWindowWarmupPredicateRuleTest
{
    private static final String YYYY_MM_DD = "yyyy-MM-dd";
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYY_MM_DD);

    @Disabled
    @Test
    public void testInRangeToday()
    {
        Instant instant = Instant.ofEpochMilli(1624245288000L); //Monday, June 21, 2021 6:14:48 AM GMT+03:00
        DateRangeSlidingWindowWarmupPredicateRule rule = new DateRangeSlidingWindowWarmupPredicateRule("c1",
                0,
                0,
                YYYY_MM_DD);
        String partitionValue = simulateDateBefore(0, instant);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();

        partitionValue = simulateDateBefore(1, instant);
        partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isFalse();
    }

    @Disabled
    @Test
    public void testInRange24Hours()
    {
        Instant instant = Instant.ofEpochMilli(1624245288000L); //Monday, June 21, 2021 6:14:48 AM GMT+03:00

        DateRangeSlidingWindowWarmupPredicateRule rule = new DateRangeSlidingWindowWarmupPredicateRule("c1",
                1,
                0,
                YYYY_MM_DD);
        String partitionValue = simulateDateBefore(0, instant);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();

        partitionValue = simulateDateBefore(1, instant);
        partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();
    }

    @Test
    public void testInRange()
    {
        DateRangeSlidingWindowWarmupPredicateRule rule = new DateRangeSlidingWindowWarmupPredicateRule("c1", 20, 10, YYYY_MM_DD);
        String partitionValue = simulateDateBefore(15);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();
    }

    @Test
    public void testOutOfRange()
    {
        DateRangeSlidingWindowWarmupPredicateRule rule = new DateRangeSlidingWindowWarmupPredicateRule("c1", 20, 10, YYYY_MM_DD);
        String partitionValue = simulateDateBefore(5);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isFalse();
    }

    @Test
    public void testUnlimitedBefore()
    {
        DateRangeSlidingWindowWarmupPredicateRule rule = new DateRangeSlidingWindowWarmupPredicateRule("c1", UNLIMITED_START_RANGE, 0, YYYY_MM_DD);
        String partitionValue = simulateDateBefore(1000);
        Map<RegularColumn, String> partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();

        rule = new DateRangeSlidingWindowWarmupPredicateRule("c1", UNLIMITED_START_RANGE, 1, YYYY_MM_DD);
        partitionValue = simulateDateBefore(1000);
        partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();

        rule = new DateRangeSlidingWindowWarmupPredicateRule("c1", UNLIMITED_START_RANGE, 10, YYYY_MM_DD);
        partitionValue = simulateDateBefore(1000);
        partitionKeys = Map.of(new RegularColumn("c1"), partitionValue);

        assertThat(rule.test(partitionKeys))
                .describedAs("partitionValue=" + partitionValue)
                .isTrue();
    }

    private String simulateDateBefore(int daysBefore)
    {
        return simulateDateBefore(daysBefore, Instant.now());
    }

    private String simulateDateBefore(int daysBefore, Instant instant)
    {
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        return simpleDateFormat.format(Date.from(instant.minus(daysBefore, ChronoUnit.DAYS)));
    }
}
