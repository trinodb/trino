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
package org.weakref.solver.type;

/**
 * Marker interface for value-level type representations produced by a preset.
 * <p>
 * Instances are created via {@link TypeConstructor#newInstance} when an embedding engine
 * wants to go from solver-level {@link org.weakref.solver.Expression} representations to its
 * own concrete Java objects (e.g. from {@code apply("decimal", literal(10), literal(2))}
 * to {@code new DecimalType(10, 2)}). The solver itself never touches this interface —
 * it's purely an instantiation API for consumers of the library.
 */
public interface Type {}
