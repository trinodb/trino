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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.RangeQuantifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationAnchor.LAST;
import static io.trino.sql.analyzer.PatternRecognitionAnalysis.NavigationMode.RUNNING;
import static java.util.Objects.requireNonNull;

public record PatternRecognitionAnalysis(Set<String> allLabels, Set<String> undefinedLabels, Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges)
{
    public PatternRecognitionAnalysis(Set<String> allLabels, Set<String> undefinedLabels, Map<NodeRef<RangeQuantifier>, Analysis.Range> ranges)
    {
        this.allLabels = requireNonNull(allLabels, "allLabels is null");
        this.undefinedLabels = ImmutableSet.copyOf(undefinedLabels);
        this.ranges = ImmutableMap.copyOf(ranges);
    }

    public record PatternInputAnalysis(Expression expression, Descriptor descriptor)
    {
        public PatternInputAnalysis
        {
            requireNonNull(expression, "expression is null");
            requireNonNull(descriptor, "descriptor is null");
        }
    }

    public enum NavigationMode
    {
        RUNNING, FINAL
    }

    public sealed interface Descriptor
            permits ScalarInputDescriptor, AggregationDescriptor, ClassifierDescriptor, MatchNumberDescriptor {}

    public record AggregationDescriptor(
            ResolvedFunction function,
            List<Expression> arguments,
            NavigationMode mode,
            Set<String> labels,
            List<FunctionCall> matchNumberCalls,
            List<FunctionCall> classifierCalls)
            implements Descriptor
    {
        public AggregationDescriptor
        {
            requireNonNull(function, "function is null");
            requireNonNull(arguments, "arguments is null");
            requireNonNull(mode, "mode is null");
            requireNonNull(labels, "labels is null");
            requireNonNull(matchNumberCalls, "matchNumberCalls is null");
            requireNonNull(classifierCalls, "classifierCalls is null");
        }
    }

    public record ScalarInputDescriptor(Optional<String> label, Navigation navigation)
            implements Descriptor
    {
        public ScalarInputDescriptor
        {
            requireNonNull(label, "label is null");
            requireNonNull(navigation, "navigation is null");
        }
    }

    public record ClassifierDescriptor(Optional<String> label, Navigation navigation)
            implements Descriptor
    {
        public ClassifierDescriptor
        {
            requireNonNull(label, "label is null");
            requireNonNull(navigation, "navigation is null");
        }
    }

    public record MatchNumberDescriptor()
            implements Descriptor {}

    public enum NavigationAnchor
    {
        FIRST, LAST
    }

    public record Navigation(NavigationAnchor anchor, NavigationMode mode, int logicalOffset, int physicalOffset)
    {
        public static final Navigation DEFAULT = new Navigation(LAST, RUNNING, 0, 0);

        public Navigation
        {
            requireNonNull(anchor, "anchor is null");
            requireNonNull(mode, "mode is null");
        }
    }
}
