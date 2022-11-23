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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

record NFA(State start, State accept, List<State> states, Map<Integer, List<Transition>> transitions)
{
    NFA {
        requireNonNull(start, "start is null");
        requireNonNull(accept, "accept is null");
        states = ImmutableList.copyOf(states);
        transitions = ImmutableMap.copyOf(transitions);
    }

    public DFA toDfa()
    {
        Map<Set<NFA.State>, DFA.State> activeStates = new HashMap<>();

        DFA.Builder builder = new DFA.Builder();
        DFA.State failed = builder.addFailState();
        for (int i = 0; i < 256; i++) {
            builder.addTransition(failed, i, failed);
        }

        Set<NFA.State> initial = transitiveClosure(Set.of(this.start));
        Queue<Set<NFA.State>> queue = new ArrayDeque<>();
        queue.add(initial);

        DFA.State dfaStartState = builder.addStartState(makeLabel(initial), initial.contains(accept));
        activeStates.put(initial, dfaStartState);

        Set<Set<NFA.State>> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            Set<NFA.State> current = queue.poll();

            if (!visited.add(current)) {
                continue;
            }

            // For each possible byte value...
            for (int byteValue = 0; byteValue < 256; byteValue++) {
                Set<NFA.State> next = new HashSet<>();
                for (NFA.State nfaState : current) {
                    for (Transition transition : transitions(nfaState)) {
                        Condition condition = transition.condition();
                        State target = states.get(transition.target());

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

                DFA.State from = activeStates.get(current);
                DFA.State to = failed;
                if (!next.isEmpty()) {
                    Set<NFA.State> closure = transitiveClosure(next);
                    to = activeStates.computeIfAbsent(closure, nfaStates -> builder.addState(makeLabel(nfaStates), nfaStates.contains(accept)));
                    queue.add(closure);
                }
                builder.addTransition(from, byteValue, to);
            }
        }

        return builder.build();
    }

    private List<Transition> transitions(State state)
    {
        return transitions.getOrDefault(state.id(), ImmutableList.of());
    }

    /**
     * Traverse epsilon transitions to compute the reachable set of states
     */
    private Set<State> transitiveClosure(Set<State> states)
    {
        Set<State> result = new HashSet<>();

        Queue<State> queue = new ArrayDeque<>(states);
        while (!queue.isEmpty()) {
            State state = queue.poll();

            if (result.contains(state)) {
                continue;
            }

            transitions(state).stream()
                    .filter(transition -> transition.condition() instanceof Epsilon)
                    .forEach(transition -> {
                        State target = this.states.get(transition.target());
                        result.add(target);
                        queue.add(target);
                    });
        }

        result.addAll(states);

        return result;
    }

    private String makeLabel(Set<NFA.State> states)
    {
        return "{" + states.stream()
                .map(NFA.State::id)
                .map(Object::toString)
                .sorted()
                .collect(Collectors.joining(",")) + "}";
    }

    public static class Builder
    {
        private int nextId;
        private State start;
        private State accept;
        private final List<State> states = new ArrayList<>();
        private final Map<Integer, List<Transition>> transitions = new HashMap<>();

        public State addState()
        {
            State state = new State(nextId++);
            states.add(state);
            return state;
        }

        public State addStartState()
        {
            checkState(start == null, "Start state is already set");
            start = addState();
            return start;
        }

        public void setAccept(State state)
        {
            checkState(accept == null, "Accept state is already set");
            accept = state;
        }

        public void addTransition(State from, Condition condition, State to)
        {
            transitions.computeIfAbsent(from.id(), key -> new ArrayList<>())
                    .add(new Transition(to.id(), condition));
        }

        public NFA build()
        {
            return new NFA(start, accept, states, transitions);
        }
    }

    public record State(int id)
    {
        @Override
        public String toString()
        {
            return "(" + id + ")";
        }
    }

    record Transition(int target, Condition condition) {}

    sealed interface Condition
            permits Epsilon, Value, Prefix
    {
    }

    record Epsilon()
            implements Condition {}

    record Value(byte value)
            implements Condition {}

    record Prefix(int prefix, int bits)
            implements Condition {}
}
