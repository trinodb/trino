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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

record DFA(State start, State failed, List<State> states, List<List<Transition>> transitions)
{
    DFA
    {
        requireNonNull(start, "start is null");
        requireNonNull(failed, "failed is null");
        states = ImmutableList.copyOf(states);
        transitions = ImmutableList.copyOf(transitions);
    }

    public List<Transition> transitions(State state)
    {
        return transitions.get(state.id);
    }

    record State(int id, String label, boolean accept)
    {
        @Override
        public String toString()
        {
            return "%s:%s%s".formatted(
                    id,
                    accept ? "*" : "",
                    label);
        }
    }

    record Transition(int value, State target)
    {
        @Override
        public String toString()
        {
            return format("-[%s]-> %s", value, target);
        }
    }

    public static class Builder
    {
        private int nextId;
        private State start;
        private State failed;
        private final List<State> states = new ArrayList<>();
        private final List<List<Transition>> transitions = new ArrayList<>();

        public State addState(String label, boolean accept)
        {
            State state = new State(nextId++, label, accept);
            states.add(state);
            transitions.add(new ArrayList<>());
            return state;
        }

        public State addStartState(String label, boolean accept)
        {
            checkState(start == null, "Start state already set");
            State state = addState(label, accept);
            start = state;
            return state;
        }

        public State addFailState()
        {
            checkState(failed == null, "Fail state already set");
            State state = addState("fail", false);
            failed = state;
            return state;
        }

        public void addTransition(State from, int value, State to)
        {
            transitions.get(from.id()).add(new Transition(value, to));
        }

        public DFA build()
        {
            return new DFA(start, failed, states, transitions);
        }
    }
}
