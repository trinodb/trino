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
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

record DFA(int start, IntArrayList acceptStates, List<List<Transition>> transitions)
{
    DFA
    {
        requireNonNull(acceptStates, "acceptStates is null");
        transitions = ImmutableList.copyOf(transitions);
    }

    record Transition(int value, int target)
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
        private int start;
        private final IntArrayList acceptStates = new IntArrayList();
        private final List<List<Transition>> transitions = new ArrayList<>();

        public int addState(boolean accept)
        {
            int state = nextId++;
            transitions.add(new ArrayList<>());
            if (accept) {
                acceptStates.add(state);
            }
            return state;
        }

        public int addStartState(boolean accept)
        {
            start = addState(accept);
            return start;
        }

        public void addTransition(int from, int value, int to)
        {
            transitions.get(from).add(new Transition(value, to));
        }

        public DFA build()
        {
            return new DFA(start, acceptStates, transitions);
        }
    }
}
