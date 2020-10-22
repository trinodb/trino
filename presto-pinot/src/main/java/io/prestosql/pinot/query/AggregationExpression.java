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
package io.prestosql.pinot.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class AggregationExpression
{
    private final String outputColumnName;
    private final String baseColumnName;
    private final String aggregationType;

    @JsonCreator
    public AggregationExpression(
            @JsonProperty("outputColumnName") String outputColumnName,
            @JsonProperty("baseColumnName") String baseColumnName,
            @JsonProperty("aggregationType") String aggregationType)
    {
        this.outputColumnName = requireNonNull(outputColumnName);
        this.baseColumnName = requireNonNull(baseColumnName);
        this.aggregationType = requireNonNull(aggregationType);
    }

    @JsonProperty
    public String getOutputColumnName()
    {
        return outputColumnName;
    }

    @JsonProperty
    public String getBaseColumnName()
    {
        return baseColumnName;
    }

    @JsonProperty
    public String getAggregationType()
    {
        return aggregationType;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }

        if (!(other instanceof AggregationExpression)) {
            return false;
        }

        AggregationExpression that = (AggregationExpression) other;
        return outputColumnName.equals(that.outputColumnName) &&
                baseColumnName.equals(that.baseColumnName) &&
                aggregationType.equals(that.aggregationType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputColumnName, baseColumnName, aggregationType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputColumnName", outputColumnName)
                .add("baseColumnName", baseColumnName)
                .add("aggregationType", aggregationType)
                .toString();
    }
}
