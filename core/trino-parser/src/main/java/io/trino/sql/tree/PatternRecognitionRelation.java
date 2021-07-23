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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionRelation
        extends Relation
{
    private final Relation input;
    private final List<Expression> partitionBy;
    private final Optional<OrderBy> orderBy;
    private final List<MeasureDefinition> measures;
    private final Optional<RowsPerMatch> rowsPerMatch;
    private final Optional<SkipTo> afterMatchSkipTo;
    private final Optional<PatternSearchMode> patternSearchMode;
    private final RowPattern pattern;
    private final List<SubsetDefinition> subsets;
    private final List<VariableDefinition> variableDefinitions;

    public PatternRecognitionRelation(
            Relation input,
            List<Expression> partitionBy,
            Optional<OrderBy> orderBy,
            List<MeasureDefinition> measures,
            Optional<RowsPerMatch> rowsPerMatch,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            RowPattern pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        this(Optional.empty(), input, partitionBy, orderBy, measures, rowsPerMatch, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    public PatternRecognitionRelation(
            NodeLocation location,
            Relation input,
            List<Expression> partitionBy,
            Optional<OrderBy> orderBy,
            List<MeasureDefinition> measures,
            Optional<RowsPerMatch> rowsPerMatch,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            RowPattern pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        this(Optional.of(location), input, partitionBy, orderBy, measures, rowsPerMatch, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    private PatternRecognitionRelation(
            Optional<NodeLocation> location,
            Relation input,
            List<Expression> partitionBy,
            Optional<OrderBy> orderBy,
            List<MeasureDefinition> measures,
            Optional<RowsPerMatch> rowsPerMatch,
            Optional<SkipTo> afterMatchSkipTo,
            Optional<PatternSearchMode> patternSearchMode,
            RowPattern pattern,
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions)
    {
        super(location);
        this.input = requireNonNull(input, "input is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.measures = requireNonNull(measures, "measures is null");
        this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        this.afterMatchSkipTo = requireNonNull(afterMatchSkipTo, "afterMatchSkipTo is null");
        this.patternSearchMode = requireNonNull(patternSearchMode, "patternSearchMode is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.subsets = requireNonNull(subsets, "subsets is null");
        requireNonNull(variableDefinitions, "variableDefinitions is null");
        checkArgument(!variableDefinitions.isEmpty(), "variableDefinitions is empty");
        this.variableDefinitions = variableDefinitions;
    }

    public Relation getInput()
    {
        return input;
    }

    public List<Expression> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy()
    {
        return orderBy;
    }

    public List<MeasureDefinition> getMeasures()
    {
        return measures;
    }

    public Optional<RowsPerMatch> getRowsPerMatch()
    {
        return rowsPerMatch;
    }

    public Optional<SkipTo> getAfterMatchSkipTo()
    {
        return afterMatchSkipTo;
    }

    public Optional<PatternSearchMode> getPatternSearchMode()
    {
        return patternSearchMode;
    }

    public RowPattern getPattern()
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
        return visitor.visitPatternRecognitionRelation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(input);
        builder.addAll(partitionBy);
        orderBy.ifPresent(builder::add);
        builder.addAll(measures);
        afterMatchSkipTo.ifPresent(builder::add);
        builder.add(pattern)
                .addAll(subsets)
                .addAll(variableDefinitions);
        patternSearchMode.ifPresent(builder::add);

        return builder.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("input", input)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy.orElse(null))
                .add("measures", measures)
                .add("rowsPerMatch", rowsPerMatch.orElse(null))
                .add("afterMatchSkipTo", afterMatchSkipTo)
                .add("patternSearchMode", patternSearchMode.orElse(null))
                .add("pattern", pattern)
                .add("subsets", subsets)
                .add("variableDefinitions", variableDefinitions)
                .omitNullValues()
                .toString();
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

        PatternRecognitionRelation that = (PatternRecognitionRelation) o;
        return Objects.equals(input, that.input) &&
                Objects.equals(partitionBy, that.partitionBy) &&
                Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(measures, that.measures) &&
                Objects.equals(rowsPerMatch, that.rowsPerMatch) &&
                Objects.equals(afterMatchSkipTo, that.afterMatchSkipTo) &&
                Objects.equals(patternSearchMode, that.patternSearchMode) &&
                Objects.equals(pattern, that.pattern) &&
                Objects.equals(subsets, that.subsets) &&
                Objects.equals(variableDefinitions, that.variableDefinitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(input, partitionBy, orderBy, measures, rowsPerMatch, afterMatchSkipTo, patternSearchMode, pattern, subsets, variableDefinitions);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return rowsPerMatch.equals(((PatternRecognitionRelation) other).rowsPerMatch);
    }

    public enum RowsPerMatch
    {
        // ONE option applies to the MATCH_RECOGNIZE clause. This is the default option.
        // Output a single summary row for every match, including empty matches.
        // In the case of an empty match, output the starting row of the match attempt.
        ONE {
            @Override
            public boolean isOneRow()
            {
                return true;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        // ALL_SHOW_EMPTY option applies to the MATCH_RECOGNIZE clause.
        // Output all rows of every match, including empty matches.
        // In the case of an empty match, output the starting row of the match attempt.
        // Do not produce output for the rows matched within exclusion `{- ... -}`.
        ALL_SHOW_EMPTY {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        // ALL_OMIT_EMPTY option applies to the MATCH_RECOGNIZE clause.
        // Output all rows of every non-empty match.
        // Do not produce output for the rows matched within exclusion `{- ... -}`
        ALL_OMIT_EMPTY {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return false;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return false;
            }
        },

        // ALL_WITH_UNMATCHED option applies to the MATCH_RECOGNIZE clause.
        // Output all rows of every match, including empty matches.
        // Produce an additional output row for every unmatched row.
        // Pattern exclusions are not allowed with this option.
        ALL_WITH_UNMATCHED {
            @Override
            public boolean isOneRow()
            {
                return false;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return true;
            }
        },

        // WINDOW option applies to pattern recognition within window specification.
        // Output one row for every input row:
        // - if the row is skipped by some previous match, produce output as for unmatched row
        // - if match is found (either empty or non-empty), output a single-row summary
        // - if no match is found, produce output as for unmatched row
        WINDOW {
            @Override
            public boolean isOneRow()
            {
                return true;
            }

            @Override
            public boolean isEmptyMatches()
            {
                return true;
            }

            @Override
            public boolean isUnmatchedRows()
            {
                return true;
            }
        };

        public abstract boolean isOneRow();

        public abstract boolean isEmptyMatches();

        public abstract boolean isUnmatchedRows();
    }
}
