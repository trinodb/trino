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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.varada.annotation.Default;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DateRangeSlidingWindowWarmupPredicateRule
        extends WarmupPredicateRule
{
    public static final String WINDOW_DATE_FORMAT = "windowDateFormat";
    public static final int UNLIMITED_START_RANGE = -1;

    private static final Logger logger = Logger.get(DateRangeSlidingWindowWarmupPredicateRule.class);

    private static final String START_RANGE_DAYS_BEFORE = "startRangeDaysBefore";
    private static final String END_RANGE_DAYS_BEFORE = "endRangeDaysBefore";

    private final int startRangeDaysBefore;
    private final int endRangeDaysBefore;
    private final String windowDateFormat;

    @Default
    @JsonCreator
    public DateRangeSlidingWindowWarmupPredicateRule(@JsonProperty(COLUMN_ID) String columnId,
            @JsonProperty(START_RANGE_DAYS_BEFORE) int startRangeDaysBefore,
            @JsonProperty(END_RANGE_DAYS_BEFORE) int endRangeDaysBefore,
            @JsonProperty(WINDOW_DATE_FORMAT) String windowDateFormat)
    {
        super(columnId);
        this.startRangeDaysBefore = startRangeDaysBefore;
        this.endRangeDaysBefore = endRangeDaysBefore;
        this.windowDateFormat = requireNonNull(windowDateFormat);
    }

    @JsonProperty(START_RANGE_DAYS_BEFORE)
    public int getStartRangeDaysBefore()
    {
        return startRangeDaysBefore;
    }

    @JsonProperty(END_RANGE_DAYS_BEFORE)
    public int getEndRangeDaysBefore()
    {
        return endRangeDaysBefore;
    }

    @JsonProperty(WINDOW_DATE_FORMAT)
    public String getWindowDateFormat()
    {
        return windowDateFormat;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateRangeSlidingWindowWarmupPredicateRule that = (DateRangeSlidingWindowWarmupPredicateRule) o;
        return startRangeDaysBefore == that.startRangeDaysBefore && endRangeDaysBefore == that.endRangeDaysBefore && Objects.equals(windowDateFormat, that.windowDateFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), startRangeDaysBefore, endRangeDaysBefore, windowDateFormat);
    }

    @Override
    public boolean test(Map<RegularColumn, String> partitionKeys)
    {
        if (partitionKeys.isEmpty()) {
            return false;
        }
        Map<String, String> partitionsByName = partitionKeys.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
        String dateString = partitionsByName.get(getColumnId());

        if (dateString == null) {
            return false;
        }

        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(windowDateFormat);
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
            Instant date = simpleDateFormat.parse(dateString).toInstant();
            Instant endRange = calculateDaysBefore(endRangeDaysBefore - 1);
            if (startRangeDaysBefore != UNLIMITED_START_RANGE) {
                Instant startRange = calculateDaysBefore(startRangeDaysBefore);
                return date.getEpochSecond() >= startRange.getEpochSecond() && date.getEpochSecond() < endRange.getEpochSecond();
            }
            else {
                return date.getEpochSecond() < endRange.getEpochSecond();
            }
        }
        catch (Throwable exception) {
            logger.warn(exception, "Could not parse date [%s] of field [%s] with format [%s], Instant=[%s]", dateString, getColumnId(), windowDateFormat, Instant.now());
            return false;
        }
    }

    private Instant calculateDaysBefore(int daysBefore)
    {
        return Instant.now()
                .minus(daysBefore, ChronoUnit.DAYS)
                .truncatedTo(ChronoUnit.DAYS);
    }

    @Override
    public String toString()
    {
        return "DateRangeSlidingWindowWarmupPredicateRule{" +
                "startRangeDaysBefore=" + startRangeDaysBefore +
                ", endRangeDaysBefore=" + endRangeDaysBefore +
                ", windowDateFormat='" + windowDateFormat + '\'' +
                "} " + super.toString();
    }
}
