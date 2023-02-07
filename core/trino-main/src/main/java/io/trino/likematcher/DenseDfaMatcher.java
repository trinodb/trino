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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

class DenseDfaMatcher
{
    // The DFA is encoded as a sequence of transitions for each possible byte value for each state.
    // I.e., 256 transitions per state.
    // The content of the transitions array is the base offset into
    // the next state to follow. I.e., the desired state * 256
    private final int[] transitions;

    // The starting state
    private final int start;

    // For each state, whether it's an accepting state
    private final boolean[] accept;

    // Artificial state to sink all invalid matches
    private final int fail;

    private final boolean exact;

    /**
     * @param exact whether to match to the end of the input
     */
    public static DenseDfaMatcher newInstance(List<Pattern> pattern, boolean exact)
    {
        DFA dfa = makeNfa(pattern).toDfa();

        int[] transitions = new int[dfa.states().size() * 256];
        boolean[] accept = new boolean[dfa.states().size()];

        for (DFA.State state : dfa.states()) {
            for (DFA.Transition transition : dfa.transitions(state)) {
                transitions[state.id() * 256 + transition.value()] = transition.target().id() * 256;
            }

            if (state.accept()) {
                accept[state.id()] = true;
            }
        }

        return new DenseDfaMatcher(transitions, dfa.start().id(), accept, 0, exact);
    }

    private DenseDfaMatcher(int[] transitions, int start, boolean[] accept, int fail, boolean exact)
    {
        this.transitions = transitions;
        this.start = start;
        this.accept = accept;
        this.fail = fail;
        this.exact = exact;
    }

    public boolean match(byte[] input, int offset, int length)
    {
        if (exact) {
            return exactMatch(input, offset, length);
        }

        return prefixMatch(input, offset, length);
    }

    /**
     * Returns a positive match when the final state after all input has been consumed is an accepting state
     */
    private boolean exactMatch(byte[] input, int offset, int length)
    {
        int state = start << 8;
        for (int i = offset; i < offset + length; i++) {
            byte inputByte = input[i];
            state = transitions[state | (inputByte & 0xFF)];

            if (state == fail) {
                return false;
            }
        }

        return accept[state >>> 8];
    }

    /**
     * Returns a positive match as soon as the DFA reaches an accepting state, regardless of whether
     * the whole input has been consumed
     */
    private boolean prefixMatch(byte[] input, int offset, int length)
    {
        int state = start << 8;
        for (int i = offset; i < offset + length; i++) {
            byte inputByte = input[i];
            state = transitions[state | (inputByte & 0xFF)];

            if (state == fail) {
                return false;
            }

            if (accept[state >>> 8]) {
                return true;
            }
        }

        return accept[state >>> 8];
    }

    private static NFA makeNfa(List<Pattern> pattern)
    {
        checkArgument(!pattern.isEmpty(), "pattern is empty");

        NFA.Builder builder = new NFA.Builder();

        NFA.State state = builder.addStartState();

        for (Pattern item : pattern) {
            if (item instanceof Pattern.Literal literal) {
                for (byte current : literal.value().getBytes(UTF_8)) {
                    state = matchByte(builder, state, current);
                }
            }
            else if (item instanceof Pattern.Any any) {
                for (int i = 0; i < any.min(); i++) {
                    NFA.State next = builder.addState();
                    matchSingleUtf8(builder, state, next);
                    state = next;
                }

                if (any.unbounded()) {
                    matchSingleUtf8(builder, state, state);
                }
            }
            else {
                throw new UnsupportedOperationException("Not supported: " + item.getClass().getName());
            }
        }

        builder.setAccept(state);

        return builder.build();
    }

    private static NFA.State matchByte(NFA.Builder builder, NFA.State state, byte value)
    {
        NFA.State next = builder.addState();
        builder.addTransition(state, new NFA.Value(value), next);
        return next;
    }

    private static void matchSingleUtf8(NFA.Builder builder, NFA.State from, NFA.State to)
    {
        /*
            Implements a state machine to recognize UTF-8 characters.

                  11110xxx       10xxxxxx       10xxxxxx       10xxxxxx
              O ───────────► O ───────────► O ───────────► O ───────────► O
              │                             ▲              ▲              ▲
              ├─────────────────────────────┘              │              │
              │          1110xxxx                          │              │
              │                                            │              │
              ├────────────────────────────────────────────┘              │
              │                   110xxxxx                                │
              │                                                           │
              └───────────────────────────────────────────────────────────┘
                                        0xxxxxxx
        */

        builder.addTransition(from, new NFA.Prefix(0, 1), to);

        NFA.State state1 = builder.addState();
        NFA.State state2 = builder.addState();
        NFA.State state3 = builder.addState();

        builder.addTransition(from, new NFA.Prefix(0b11110, 5), state1);
        builder.addTransition(from, new NFA.Prefix(0b1110, 4), state2);
        builder.addTransition(from, new NFA.Prefix(0b110, 3), state3);

        builder.addTransition(state1, new NFA.Prefix(0b10, 2), state2);
        builder.addTransition(state2, new NFA.Prefix(0b10, 2), state3);
        builder.addTransition(state3, new NFA.Prefix(0b10, 2), to);
    }
}
