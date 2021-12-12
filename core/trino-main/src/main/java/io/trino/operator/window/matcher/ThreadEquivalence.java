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
package io.trino.operator.window.matcher;

import com.google.common.collect.ImmutableList;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MatchAggregation;
import io.trino.operator.window.pattern.MatchAggregationPointer;
import io.trino.operator.window.pattern.PhysicalValueAccessor;
import io.trino.operator.window.pattern.PhysicalValuePointer;
import io.trino.sql.planner.LocalExecutionPlanner.MatchAggregationLabelDependency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;

/**
 * The purpose of this class is to determine whether two pattern matching threads
 * are equivalent. Based on the thread equivalence, threads which are duplicates
 * of some other thread can be pruned.
 * <p>
 * It is assumed that the two compared threads:
 * - have already matched the same portion of input. This also means that
 * their corresponding arrays of matched labels are of equal lengths.
 * - have reached the same instruction in the program.
 * <p>
 * It takes the following steps to determine if two threads are equivalent:
 * <p>
 * 1. get the set of labels reachable by the program from the current
 * instruction until the end of the program (`reachableLabels`)
 * <p>
 * 2. for all those labels, get all navigating operations for accessing
 * input values which need to be performed during label evaluation
 * (`positionsToCompare`). For all those navigating operations, check if they
 * return the same result (i.e. navigate to the same input row) for both threads.
 * NOTE: the computations can be simplified by stripping the physical offsets
 * and skipping navigations which refer to the universal pattern variable.
 * <p>
 * 3. for all those labels, get all navigating operations for `CLASSIFIER` calls
 * which need to be performed during label evaluation (`labelsToCompare`).
 * For all those navigating operations, check if they navigate to rows tagged
 * with the same label (but not necessarily the same position in input) for both threads.
 * <p>
 * NOTE: the navigating operations for `MATCH_NUMBER` calls can be skipped
 * altogether, since the match number is constant in this context.
 * <p>
 * 4. for all those labels, get all aggregations which need to be computed
 * during label evaluation.
 * <p>
 * 4a. for aggregations whose arguments do not depend on the actual matched labels,
 * check if the aggregated positions are the same for both threads.
 * <p>
 * 4b. for aggregations whose arguments depend on the actual matched labels,
 * check if the aggregated positions, and the assigned labels are the same for both threads.
 */
public class ThreadEquivalence
{
    // for every pointer (instruction) in the program, the set of labels reachable by the program
    // starting from this instruction until the program ends, through any path.
    private final List<Set<Integer>> reachableLabels;

    // for every label, the set of navigations for accessing input values based on the defining condition
    private final List<Set<LogicalIndexNavigation>> positionsToCompare;

    // for every label, the set of navigations for accessing the matched labels based on the defining condition
    private final List<Set<LogicalIndexNavigation>> labelsToCompare;

    // for every label, the list indexes of all aggregations present in the defining condition
    // which require only comparing the aggregated positions
    private final List<List<Integer>> matchAggregationsToComparePositions;

    // for every label, the list of indexes of all aggregations present in the defining condition
    // which require comparing the aggregated positions and assigned labels
    private final List<List<Integer>> matchAggregationsToComparePositionsAndLabels;

    public ThreadEquivalence(Program program, List<List<PhysicalValueAccessor>> accessors, List<MatchAggregationLabelDependency> labelDependencies)
    {
        this.reachableLabels = computeReachableLabels(program);

        this.positionsToCompare = getInputValuePointers(accessors).stream()
                .map(pointersList -> pointersList.stream()
                        .map(PhysicalValuePointer::getLogicalIndexNavigation)
                        .filter(navigation -> !navigation.getLabels().isEmpty())
                        .map(LogicalIndexNavigation::withoutPhysicalOffset)
                        .map(ThreadEquivalence::allPositionsToCompare)
                        .flatMap(Collection::stream)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        this.labelsToCompare = getClassifierValuePointers(accessors).stream()
                .map(pointersList -> pointersList.stream()
                        .map(PhysicalValuePointer::getLogicalIndexNavigation)
                        .map(ThreadEquivalence::allPositionsToCompare)
                        .flatMap(Collection::stream)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        AggregationIndexes aggregationIndexes = classifyAggregations(accessors, labelDependencies);
        this.matchAggregationsToComparePositions = aggregationIndexes.foundNoClassifierAggregations ? aggregationIndexes.noClassifierAggregations : null;
        this.matchAggregationsToComparePositionsAndLabels = aggregationIndexes.foundClassifierAggregations ? aggregationIndexes.classifierAggregations : null;
    }

    public boolean equivalent(int firstThread, ArrayView firstLabels, MatchAggregation[] firstAggregations, int secondThread, ArrayView secondLabels, MatchAggregation[] secondAggregations, int pointer)
    {
        checkArgument(firstLabels.length() == secondLabels.length(), "matched labels for compared threads differ in length");
        checkArgument(pointer >= 0 && pointer < reachableLabels.size(), "instruction pointer out of program bounds");

        if (firstThread == secondThread || firstLabels.length() == 0) {
            return true;
        }

        // compare resulting positions for input navigations
        Set<LogicalIndexNavigation> distinctPositionsToCompare = new HashSet<>();
        for (int label : reachableLabels.get(pointer)) {
            distinctPositionsToCompare.addAll(positionsToCompare.get(label));
        }
        for (LogicalIndexNavigation navigation : distinctPositionsToCompare) {
            if (resolvePosition(navigation, firstLabels) != resolvePosition(navigation, secondLabels)) {
                return false;
            }
        }

        // compare resulting labels for `CLASSIFIER` navigations
        Set<LogicalIndexNavigation> distinctLabelPositionsToCompare = new HashSet<>();
        for (int label : reachableLabels.get(pointer)) {
            distinctLabelPositionsToCompare.addAll(labelsToCompare.get(label));
        }
        for (LogicalIndexNavigation navigation : distinctLabelPositionsToCompare) {
            int firstPosition = resolvePosition(navigation, firstLabels);
            int secondPosition = resolvePosition(navigation, secondLabels);
            if ((firstPosition == -1) != (secondPosition == -1)) {
                return false;
            }
            if (firstPosition != -1 && firstLabels.get(firstPosition) != secondLabels.get(secondPosition)) {
                return false;
            }
        }

        // compare sets of all aggregated positions for aggregations which do not depend on `CLASSIFIER`
        if (matchAggregationsToComparePositions != null) {
            Set<Integer> aggregationsToComparePositions = new HashSet<>();
            for (int label : reachableLabels.get(pointer)) {
                aggregationsToComparePositions.addAll(matchAggregationsToComparePositions.get(label));
            }
            for (int aggregationIndex : aggregationsToComparePositions) {
                ArrayView firstPositions = firstAggregations[aggregationIndex].getAllPositions(firstLabels);
                ArrayView secondPositions = secondAggregations[aggregationIndex].getAllPositions(secondLabels);
                if (firstPositions.length() != secondPositions.length()) {
                    return false;
                }
                for (int i = 0; i < firstPositions.length(); i++) {
                    if (firstPositions.get(i) != secondPositions.get(i)) {
                        return false;
                    }
                }
            }
        }

        // compare sets of all aggregated positions, and sets of matched labels on all aggregated positions for aggregations which depend on `CLASSIFIER`
        if (matchAggregationsToComparePositionsAndLabels != null) {
            Set<Integer> aggregationsToComparePositionsAndLabels = new HashSet<>();
            for (int label : reachableLabels.get(pointer)) {
                aggregationsToComparePositionsAndLabels.addAll(matchAggregationsToComparePositionsAndLabels.get(label));
            }
            for (int aggregationIndex : aggregationsToComparePositionsAndLabels) {
                ArrayView firstPositions = firstAggregations[aggregationIndex].getAllPositions(firstLabels);
                ArrayView secondPositions = secondAggregations[aggregationIndex].getAllPositions(secondLabels);
                if (firstPositions.length() != secondPositions.length()) {
                    return false;
                }
                for (int i = 0; i < firstPositions.length(); i++) {
                    int position = firstPositions.get(i);
                    if (position != secondPositions.get(i) || firstLabels.get(position) != secondLabels.get(position)) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static int resolvePosition(LogicalIndexNavigation navigation, ArrayView labels)
    {
        return navigation.resolvePosition(labels.length() - 1, labels, 0, labels.length(), 0);
    }

    private static List<Set<Integer>> computeReachableLabels(Program program)
    {
        List<Set<Integer>> reachableLabels = new ArrayList<>(program.size());

        // because the program might have cycles, the computation is done for every instruction
        // TODO optimize the computations to reuse the results whenever possible
        for (int instructionIndex = 0; instructionIndex < program.size(); instructionIndex++) {
            reachableLabels.add(reachableLabels(program, instructionIndex, new boolean[program.size()]));
        }

        return reachableLabels;
    }

    private static Set<Integer> reachableLabels(Program program, int instructionIndex, boolean[] visited)
    {
        if (visited[instructionIndex]) {
            return new HashSet<>();
        }

        visited[instructionIndex] = true;

        Set<Integer> reachableLabels = new HashSet<>();
        Instruction instruction = program.at(instructionIndex);
        switch (instruction.type()) {
            case MATCH_LABEL:
                reachableLabels.addAll(reachableLabels(program, instructionIndex + 1, visited));
                reachableLabels.add(((MatchLabel) instruction).getLabel());
                break;
            case JUMP:
                reachableLabels.addAll(reachableLabels(program, ((Jump) instruction).getTarget(), visited));
                break;
            case SPLIT:
                reachableLabels.addAll(reachableLabels(program, ((Split) instruction).getFirst(), visited));
                reachableLabels.addAll(reachableLabels(program, ((Split) instruction).getSecond(), visited));
                break;
            case MATCH_START:
            case MATCH_END:
            case SAVE:
                reachableLabels.addAll(reachableLabels(program, instructionIndex + 1, visited));
                break;
            case DONE:
                // no reachable labels
        }

        return reachableLabels;
    }

    private static List<List<PhysicalValuePointer>> getInputValuePointers(List<List<PhysicalValueAccessor>> valuePointers)
    {
        return valuePointers.stream()
                .map(pointerList -> pointerList.stream()
                        .filter(pointer -> pointer instanceof PhysicalValuePointer)
                        .map(PhysicalValuePointer.class::cast)
                        .filter(pointer -> pointer.getSourceChannel() != CLASSIFIER && pointer.getSourceChannel() != MATCH_NUMBER)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private static List<List<PhysicalValuePointer>> getClassifierValuePointers(List<List<PhysicalValueAccessor>> valuePointers)
    {
        return valuePointers.stream()
                .map(pointerList -> pointerList.stream()
                        .filter(pointer -> pointer instanceof PhysicalValuePointer)
                        .map(PhysicalValuePointer.class::cast)
                        .filter(pointer -> pointer.getSourceChannel() == CLASSIFIER)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    // for every label, iterate over aggregations in the label's defining condition, and divide them into sublists:
    // - aggregations which do not depend on `CLASSIFIER`, that is, either do not use `CLASSIFIER` in any of their arguments,
    // or apply to only one label, in which case the result of `CLASSIFIER` is always the same.
    // - aggregations which depend on `CLASSIFIER`, that is, use `CLASSIFIER` in some of their arguments,
    // and apply to more than one label (incl. the universal pattern variable), in which case the result of `CLASSIFIER`
    // depends on the actual matched label.
    private static AggregationIndexes classifyAggregations(List<List<PhysicalValueAccessor>> valuePointers, List<MatchAggregationLabelDependency> labelDependencies)
    {
        ImmutableList.Builder<List<Integer>> noClassifierAggregations = ImmutableList.builder();
        boolean foundNoClassifierAggregations = false;
        ImmutableList.Builder<List<Integer>> classifierAggregations = ImmutableList.builder();
        boolean foundClassifierAggregations = false;

        for (List<PhysicalValueAccessor> pointerList : valuePointers) {
            ImmutableList.Builder<Integer> noClassifierAggregationIndexes = ImmutableList.builder();
            ImmutableList.Builder<Integer> classifierAggregationIndexes = ImmutableList.builder();
            for (PhysicalValueAccessor pointer : pointerList) {
                if (pointer instanceof MatchAggregationPointer) {
                    int aggregationIndex = ((MatchAggregationPointer) pointer).getIndex();
                    MatchAggregationLabelDependency labelDependency = labelDependencies.get(aggregationIndex);
                    if (!labelDependency.isClassifierInvolved() || labelDependency.getLabels().size() == 1) {
                        foundNoClassifierAggregations = true;
                        noClassifierAggregationIndexes.add(aggregationIndex);
                    }
                    else {
                        foundClassifierAggregations = true;
                        classifierAggregationIndexes.add(aggregationIndex);
                    }
                }
            }
            noClassifierAggregations.add(noClassifierAggregationIndexes.build());
            classifierAggregations.add(classifierAggregationIndexes.build());
        }
        return new AggregationIndexes(foundNoClassifierAggregations, noClassifierAggregations.build(), foundClassifierAggregations, classifierAggregations.build());
    }

    /**
     * For a LogicalIndexNavigation, returns a set of all navigations which must return
     * equal results for the two compared threads if the threads are equivalent.
     * <p>
     * FIRST(A.value) -> compare the position "FIRST(A)"
     * FIRST(A.value, 2) -> compare the position "FIRST(A, 2)"
     * LAST(A.value) -> compare the position "LAST(A)"
     * LAST(A.value, 2) -> compare the positions "LAST(A, 2)", "LAST(A, 1)", "LAST(A)".
     * They must all be equal for both threads in case there are more labels "A" assigned in the future.
     * <p>
     * PREV(LAST(CLASSIFIER(A), 2), 5) -> compare the positions "PREV(LAST(A, 2), 5)", "PREV(LAST(A, 1), 5)", "PREV(LAST(A), 5)",
     * and the 5 trailing labels. They must all be equal for both threads in case there are more labels "A" assigned in the future.
     */
    private static List<LogicalIndexNavigation> allPositionsToCompare(LogicalIndexNavigation navigation)
    {
        if (navigation.isLast()) {
            List<LogicalIndexNavigation> result = new ArrayList<>();
            for (int offset = 0; offset <= navigation.getLogicalOffset(); offset++) {
                result.add(navigation.withLogicalOffset(offset));
            }

            // physical offset can be present only in `CLASSIFIER` navigations. For input navigations it was pruned.
            // In case when the physical offset is negative, we need to compare all labels in the offset-length suffix
            // of the match between both compared threads.
            for (int tail = navigation.getPhysicalOffset() + 1; tail < 0; tail++) {
                result.add(navigation.withoutLogicalOffset().withPhysicalOffset(tail));
            }
            return result;
        }

        return ImmutableList.of(navigation);
    }

    private static class AggregationIndexes
    {
        final boolean foundNoClassifierAggregations;
        final List<List<Integer>> noClassifierAggregations;
        final boolean foundClassifierAggregations;
        final List<List<Integer>> classifierAggregations;

        public AggregationIndexes(boolean foundNoClassifierAggregations, List<List<Integer>> noClassifierAggregations, boolean foundClassifierAggregations, List<List<Integer>> classifierAggregations)
        {
            this.foundNoClassifierAggregations = foundNoClassifierAggregations;
            this.noClassifierAggregations = noClassifierAggregations;
            this.foundClassifierAggregations = foundClassifierAggregations;
            this.classifierAggregations = classifierAggregations;
        }
    }
}
