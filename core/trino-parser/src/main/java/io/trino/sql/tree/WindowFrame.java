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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowFrame
        extends Node
{
    public enum Type
    {
        RANGE, ROWS, GROUPS
    }

    private final Type type;
    private final FrameBound start;
    private final Optional<FrameBound> end;
    private final List<MeasureDefinition> measures;
    private final Optional<SkipTo> afterMatchSkipTo;
    private final Optional<PatternSearchMode> patternSearchMode;
    private final Optional<RowPattern> pattern;
    private final List<SubsetDefinition> subsets;
    private final List<VariableDefinition> variableDefinitions;

    public WindowFrame(
            Type type,
            FrameBound start,
            Optional<FrameBound> end,
            List<MeasureDefinition> measures,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            Optional<RowPattern> pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        this(Optional.empty(), type, start, end, measures, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    public WindowFrame(
            NodeLocation location,
            Type type,
            FrameBound start,
            Optional<FrameBound> end,
            List<MeasureDefinition> measures,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            Optional<RowPattern> pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        this(Optional.of(location), type, start, end, measures, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    private WindowFrame(
            Optional<NodeLocation> location,
            Type type,
            FrameBound start,
            Optional<FrameBound> end,
            List<MeasureDefinition> measures,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            Optional<RowPattern> pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.start = requireNonNull(start, "start is null");
        this.end = requireNonNull(end, "end is null");
        this.measures = requireNonNull(measures, "measures is null");
        this.afterMatchSkipTo = requireNonNull(afterMatchSkipTo, "afterMatchSkipTo is null");
        this.patternSearchMode = requireNonNull(patternSearchMode, "patternSearchMode is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.subsets = requireNonNull(subsets, "subsets is null");
        this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
    }

    public Type getType()
    {
        return type;
    }

    public FrameBound getStart()
    {
        return start;
    }

    public Optional<FrameBound> getEnd()
    {
        return end;
    }

    public List<MeasureDefinition> getMeasures()
    {
        return measures;
    }

    public Optional<SkipTo> getAfterMatchSkipTo()
    {
        return afterMatchSkipTo;
    }

    public Optional<PatternSearchMode> getPatternSearchMode()
    {
        return patternSearchMode;
    }

    public Optional<RowPattern> getPattern()
    {
        return pattern;
    }

    public List<SubsetDefinition> getSubsets()
    {
        return subsets;
    }

    public List<VariableDefinition> getVariableDefinitions()
    {
        return variableDefinitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowFrame(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(start);
        end.ifPresent(nodes::add);
        nodes.addAll(measures);
        afterMatchSkipTo.ifPresent(nodes::add);
        patternSearchMode.ifPresent(nodes::add);
        pattern.ifPresent(nodes::add);
        nodes.addAll(subsets);
        nodes.addAll(variableDefinitions);
        return nodes.build();
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
        WindowFrame o = (WindowFrame) obj;
        return type == o.type &&
                Objects.equals(start, o.start) &&
                Objects.equals(end, o.end) &&
                Objects.equals(measures, o.measures) &&
                Objects.equals(afterMatchSkipTo, o.afterMatchSkipTo) &&
                Objects.equals(patternSearchMode, o.patternSearchMode) &&
                Objects.equals(pattern, o.pattern) &&
                Objects.equals(subsets, o.subsets) &&
                Objects.equals(variableDefinitions, o.variableDefinitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, start, end, measures, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("start", start)
                .add("end", end)
                .add("measures", measures)
                .add("afterMatchSkipTo", afterMatchSkipTo)
                .add("patternSearchMode", patternSearchMode)
                .add("pattern", pattern)
                .add("subsets", subsets)
                .add("variableDefinitions", variableDefinitions)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        WindowFrame otherNode = (WindowFrame) other;
        return type == otherNode.type;
    }
}
