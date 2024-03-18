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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = WarmupPredicateRule.TYPE)
@JsonSubTypes({
        @JsonSubTypes.Type(value = PartitionValueWarmupPredicateRule.class, name = "PartitionValue"),
        @JsonSubTypes.Type(value = DateSlidingWindowWarmupPredicateRule.class, name = "DateSlidingWindow"),
        @JsonSubTypes.Type(value = DateRangeSlidingWindowWarmupPredicateRule.class, name = "DateRangeSlidingWindow")
})
public abstract class WarmupPredicateRule
        implements Predicate<Map<RegularColumn, String>>, Serializable
{
    public static final String TYPE = "type";
    public static final String COLUMN_ID = "column_id";

    private final String columnId;

    protected WarmupPredicateRule(String columnId)
    {
        this.columnId = requireNonNull(columnId);
    }

    @JsonProperty(COLUMN_ID)
    public String getColumnId()
    {
        return columnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WarmupPredicateRule that)) {
            return false;
        }
        return Objects.equals(getColumnId(), that.getColumnId());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getColumnId());
    }

    @Override
    public String toString()
    {
        return "WarmupPredicateRule{" +
                "columnId='" + columnId + '\'' +
                '}';
    }
}
