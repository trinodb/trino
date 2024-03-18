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
import io.trino.plugin.varada.dispatcher.model.RegularColumn;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PartitionValueWarmupPredicateRule
        extends WarmupPredicateRule
{
    public static final String VALUE = "value";

    private final String value;

    @JsonCreator
    public PartitionValueWarmupPredicateRule(@JsonProperty(COLUMN_ID) String columnId,
            @JsonProperty(VALUE) String value)
    {
        super(columnId);
        this.value = requireNonNull(value);
    }

    @JsonProperty(VALUE)
    public String getValue()
    {
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionValueWarmupPredicateRule that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public String toString()
    {
        return "PartitionValueWarmupPredicateRule{" +
                "value=" + value +
                ", " + super.toString() +
                '}';
    }

    @Override
    public boolean test(Map<RegularColumn, String> partitionKeys)
    {
        Map<String, String> partitionsByName = partitionKeys.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
        String partitionValue = partitionsByName.get(getColumnId());
        return (partitionValue != null) && partitionValue.equals(value);
    }
}
