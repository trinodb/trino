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
 * Classifies what a solver variable represents.
 * <ul>
 *   <li>{@link #TYPE} — the variable ranges over type expressions (symbols, applications, rows).</li>
 *   <li>{@link #NUMBER} — the variable ranges over integers, bounded by min/max.</li>
 * </ul>
 * Variables of different kinds never unify. The kind is inferred from the context the
 * variable first appears in (e.g. a position expecting a type vs a numeric literal position),
 * and can be asserted explicitly via {@link RequireKind}.
 */
public enum Kind
{
    TYPE, NUMBER
}
