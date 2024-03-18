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

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

public class DateSlidingWindowWarmupPredicateRule
        extends WarmupPredicateRule
{
    public static final String WINDOW_SIZE_DAYS = "window_size_days";
    public static final String WINDOW_DATE_FORMAT = "window_date_format";
    public static final String BASE_DATE = "base_date";
    private static final Logger logger = Logger.get(DateSlidingWindowWarmupPredicateRule.class);
    private final int windowSizeDays;
    private final String windowDateFormat;
    private final String baseDate;

    @JsonCreator
    public DateSlidingWindowWarmupPredicateRule(@JsonProperty(COLUMN_ID) String columnId,
            @JsonProperty(WINDOW_SIZE_DAYS) int windowSizeDays,
            @JsonProperty(WINDOW_DATE_FORMAT) String windowDateFormat,
            @JsonProperty(BASE_DATE) String baseDate)
    {
        super(columnId);
        this.windowSizeDays = windowSizeDays;
        this.windowDateFormat = requireNonNull(windowDateFormat);
        this.baseDate = (baseDate == null || baseDate.isEmpty()) ? null : baseDate;
    }

    @JsonProperty(WINDOW_SIZE_DAYS)
    public int getWindowSizeDays()
    {
        return windowSizeDays;
    }

    @JsonProperty(WINDOW_DATE_FORMAT)
    public String getWindowDateFormat()
    {
        return windowDateFormat;
    }

    @JsonProperty(BASE_DATE)
    public String getBaseDate()
    {
        return baseDate;
    }

    @Override
    public boolean test(Map<RegularColumn, String> partitionKeys)
    {
        if (partitionKeys.isEmpty()) {
            return false;
        }
        Map<String, String> partitionsByName = partitionKeys.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getName(), Entry::getValue));
        String dateString = partitionsByName.get(getColumnId());
        if (dateString == null) {
            return false;
        }

        try {
            LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern(windowDateFormat));
            LocalDate baseDate = this.baseDate != null ? getParsedBaseDate() : LocalDate.now(ZoneId.systemDefault()); // if not baseDate use today
            LocalDate windowSizeAgo = calculateDaysBefore(baseDate);

            return !date.isAfter(baseDate) && !date.isBefore(windowSizeAgo);
        }
        catch (DateTimeParseException exception) {
            logger.warn("Could not parse date %s of field %s with format %s", dateString, getColumnId(), windowDateFormat);
            return false;
        }
    }

    private LocalDate calculateDaysBefore(LocalDate baseDate)
    {
        return requireNonNullElseGet(baseDate, LocalDate::now).minusDays(windowSizeDays);
    }

    private LocalDate getParsedBaseDate()
            throws DateTimeParseException
    {
        try {
            if (this.baseDate != null) {
                return LocalDate.parse(this.baseDate, DateTimeFormatter.ofPattern(windowDateFormat));
            }
        }
        catch (DateTimeParseException e) {
            logger.warn("failed to parse base date: %s with following format: %s", baseDate, windowDateFormat);
            throw e;
        }
        return null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DateSlidingWindowWarmupPredicateRule that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return getWindowSizeDays() == that.getWindowSizeDays() &&
                Objects.equals(getWindowDateFormat(), that.getWindowDateFormat()) &&
                Objects.equals(baseDate, that.baseDate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), windowSizeDays, windowDateFormat, baseDate);
    }

    @Override
    public String toString()
    {
        return "DateSlidingWindowWarmupPredicateRule{" +
                "windowSizeDays=" + windowSizeDays +
                ", windowDateFormat='" + windowDateFormat + '\'' +
                ", baseDate='" + baseDate + '\'' +
                ", " + super.toString() +
                '}';
    }
}
