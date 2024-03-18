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

public class DateRangeSlidingWindowWarmupPredicateRule
        extends WarmupPredicateRule
{
    public static final String WINDOW_DATE_FORMAT = "windowDateFormat";

    private static final String START_RANGE_DAYS_BEFORE = "startRangeDaysBefore";
    private static final String END_RANGE_DAYS_BEFORE = "endRangeDaysBefore";
    private final int startRangeDaysBefore;
    private final int endRangeDaysBefore;
    private final String windowDateFormat;

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
        if (!(o instanceof DateRangeSlidingWindowWarmupPredicateRule that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return getStartRangeDaysBefore() == that.getStartRangeDaysBefore() &&
                getEndRangeDaysBefore() == that.getEndRangeDaysBefore() &&
                Objects.equals(getWindowDateFormat(), that.getWindowDateFormat());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), getStartRangeDaysBefore(), getEndRangeDaysBefore(), getWindowDateFormat());
    }

    @Override
    public String toString()
    {
        return "DateRangeSlidingWindowWarmupPredicateRule{" +
                "startRangeDaysBefore=" + startRangeDaysBefore +
                ", endRangeDaysBefore=" + endRangeDaysBefore +
                ", windowDateFormat='" + windowDateFormat + '\'' +
                '}';
    }
}
