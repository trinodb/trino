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
package io.trino.likematcher;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

final class NFA
{
    private final int start;
    private final int accept;
    private final List<List<Transition>> transitions;

    private NFA(int start, int accept, List<List<Transition>> transitions)
    {
        this.start = start;
        this.accept = accept;
        this.transitions = requireNonNull(transitions, "transitions is null");
    }

    public DFA toDfa()
    {
        Map<IntSet, Integer> activeStates = new HashMap<>();

        DFA.Builder builder = new DFA.Builder();

        IntSet initial = new IntArraySet();
        initial.add(start);
        Queue<IntSet> queue = new ArrayDeque<>();
        queue.add(initial);

        int dfaStartState = builder.addStartState(initial.contains(accept));
        activeStates.put(initial, dfaStartState);

        Set<IntSet> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            IntSet current = queue.poll();

            if (!visited.add(current)) {
                continue;
            }

            // For each possible byte value...
            for (int byteValue = 0; byteValue < 256; byteValue++) {
                IntSet next = new IntArraySet();
                for (int nfaState : current) {
                    for (Transition transition : transitions(nfaState)) {
                        Condition condition = transition.condition();
                        int target = transition.target();

                        if (condition instanceof Value valueTransition && valueTransition.value() == (byte) byteValue) {
                            next.add(target);
                        }
                        else if (condition instanceof Prefix prefixTransition) {
                            if (byteValue >>> (8 - prefixTransition.bits()) == prefixTransition.prefix()) {
                                next.add(target);
                            }
                        }
                    }
                }

                if (!next.isEmpty()) {
                    int from = activeStates.get(current);
                    int to = activeStates.computeIfAbsent(next, nfaStates -> builder.addState(nfaStates.contains(accept)));
                    builder.addTransition(from, byteValue, to);

                    queue.add(next);
                }
            }
        }

        return builder.build();
    }

    private List<Transition> transitions(int state)
    {
        return transitions.get(state);
    }

    public static class Builder
    {
        private int nextId;
        private int start;
        private int accept;
        private final List<List<Transition>> transitions = new ArrayList<>();

        public int addState()
        {
            transitions.add(new ArrayList<>());
            return nextId++;
        }

        public int addStartState()
        {
            start = addState();
            return start;
        }

        public void setAccept(int state)
        {
            accept = state;
        }

        public void addTransition(int from, Condition condition, int to)
        {
            transitions.get(from).add(new Transition(to, condition));
        }

        public NFA build()
        {
            return new NFA(start, accept, transitions);
        }
    }

    record Transition(int target, Condition condition) {}

    sealed interface Condition
            permits Value, Prefix
    {
    }

    record Value(byte value)
            implements Condition {}

    record Prefix(int prefix, int bits)
            implements Condition {}
}
