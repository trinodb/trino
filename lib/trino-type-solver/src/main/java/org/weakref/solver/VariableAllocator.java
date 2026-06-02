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
 */
public class VariableAllocator
{
    private int counter;

    public String newVariable()
    {
        counter++;
        return "@v" + counter;
    }
}
