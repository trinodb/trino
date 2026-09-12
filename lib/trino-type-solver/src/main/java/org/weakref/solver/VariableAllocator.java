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
package org.weakref.solver;

/**
 * Monotonic source of fresh variable names ({@code @v1}, {@code @v2}, ...).
 * <p>
 * Used wherever the solver or a coercion rule needs to open a scheme with variables
 * that cannot collide with anything already in flight. Two invocations of the same rule
 * on the same input must use independently-allocated variables so their constraints
 * don't interfere.
 * <p>
 * The no-collision guarantee only holds against names this same allocator minted. When a
 * solve consumes constraints whose variables were minted by a <em>different</em> allocator
 * (which also started at {@code @v1}), the consumer must {@link #reserveThrough(int) reserve}
 * past those ids first, or the two id spaces overlap and unification conflates distinct
 * variables.
 */
public class VariableAllocator
{
    private int counter;

    public String newVariable()
    {
        counter++;
        return "@v" + counter;
    }

    /**
     * Advance the counter so every subsequently-minted variable has an id strictly greater
     * than {@code id}. Seeds this allocator past variables produced elsewhere so the names
     * it goes on to mint stay disjoint from them.
     *
     * @param id the highest foreign variable id that must not be re-issued
     */
    public void reserveThrough(int id)
    {
        counter = Math.max(counter, id);
    }

    /**
     * Parse the numeric id of an allocator-minted name, e.g. {@code "@v7"} → {@code 7}.
     * Returns {@code 0} for any name not of that form.
     *
     * @param name the variable name
     * @return the numeric id, or {@code 0} when {@code name} was not minted by this allocator
     */
    public static int variableId(String name)
    {
        if (!name.startsWith("@v")) {
            return 0;
        }
        try {
            return Integer.parseInt(name.substring(2));
        }
        catch (NumberFormatException e) {
            return 0;
        }
    }
}
