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
package io.trino.spi.function;

import static java.util.Objects.requireNonNull;

public record AggregationDecomposition(String partial, String output)
{
    public AggregationDecomposition
    {
        requireNonNull(partial, "partial cannot be null");
        if (partial.isEmpty()) {
            throw new IllegalArgumentException("partial cannot be empty");
        }
        requireNonNull(output, "output cannot be null");
        if (output.isEmpty()) {
            throw new IllegalArgumentException("output cannot be empty");
        }
    }
}
