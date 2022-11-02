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
package io.trino.sql.planner.rowpattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.sql.planner.rowpattern.ir.IrLabel;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AggregatedSetDescriptor
{
    // A set of labels to identify all rows to aggregate over:
    // avg(A.price) => this is an aggregation over rows with label A, so labels = {A}
    // avg(Union.price) => this is an aggregation over rows matching a union variable Union, so for SUBSET Union = (A, B, C), labels = {A, B, C}
    // avg(price) => this is an aggregation over "universal pattern variable", which is effectively over all rows, no matter the assigned labels. In such case labels = {}
    private final Set<IrLabel> labels;

    private final boolean running;

    @JsonCreator
    public AggregatedSetDescriptor(Set<IrLabel> labels, boolean running)
    {
        this.labels = requireNonNull(labels, "labels is null");
        this.running = running;
    }

    @JsonProperty
    public Set<IrLabel> getLabels()
    {
        return labels;
    }

    @JsonProperty
    public boolean isRunning()
    {
        return running;
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

        AggregatedSetDescriptor that = (AggregatedSetDescriptor) o;
        return labels.equals(that.labels) &&
                running == that.running;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(labels, running);
    }
}
