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
import io.trino.sql.planner.Symbol;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ScalarValuePointer
        implements ValuePointer
{
    private final LogicalIndexPointer logicalIndexPointer;
    private final Symbol inputSymbol;

    @JsonCreator
    public ScalarValuePointer(LogicalIndexPointer logicalIndexPointer, Symbol inputSymbol)
    {
        this.logicalIndexPointer = requireNonNull(logicalIndexPointer, "logicalIndexPointer is null");
        this.inputSymbol = requireNonNull(inputSymbol, "inputSymbol is null");
    }

    @JsonProperty
    public LogicalIndexPointer getLogicalIndexPointer()
    {
        return logicalIndexPointer;
    }

    @JsonProperty
    public Symbol getInputSymbol()
    {
        return inputSymbol;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ScalarValuePointer o = (ScalarValuePointer) obj;
        return Objects.equals(logicalIndexPointer, o.logicalIndexPointer) &&
                Objects.equals(inputSymbol, o.inputSymbol);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(logicalIndexPointer, inputSymbol);
    }
}
