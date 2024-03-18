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
package io.trino.plugin.varada.api.warmup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DateSlidingWindowWarmupPredicateRule
        extends WarmupPredicateRule
{
    public static final String WINDOW_SIZE_DAYS = "windowSizeDays";
    public static final String WINDOW_DATE_FORMAT = "windowDateFormat";
    public static final String BASE_DATE = "baseDate";

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
        return getWindowSizeDays() == that.getWindowSizeDays() && Objects.equals(getWindowDateFormat(), that.getWindowDateFormat()) && Objects.equals(getBaseDate(), that.getBaseDate());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), getWindowSizeDays(), getWindowDateFormat(), getBaseDate());
    }

    @Override
    public String toString()
    {
        return "DateSlidingWindowWarmupPredicateRule{" +
                "windowSizeDays=" + windowSizeDays +
                ", windowDateFormat='" + windowDateFormat + '\'' +
                ", baseDate='" + baseDate + '\'' +
                '}';
    }
}
