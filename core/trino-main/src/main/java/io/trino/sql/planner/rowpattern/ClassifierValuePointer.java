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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ClassifierValuePointer
        implements ValuePointer
{
    private final LogicalIndexPointer logicalIndexPointer;

    @JsonCreator
    public ClassifierValuePointer(LogicalIndexPointer logicalIndexPointer)
    {
        this.logicalIndexPointer = requireNonNull(logicalIndexPointer, "logicalIndexPointer is null");
    }

    @JsonProperty
    public LogicalIndexPointer getLogicalIndexPointer()
    {
        return logicalIndexPointer;
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
        ClassifierValuePointer that = (ClassifierValuePointer) o;
        return Objects.equals(logicalIndexPointer, that.logicalIndexPointer);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logicalIndexPointer);
    }
}
