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

import io.trino.spi.predicate.Domain;

import static java.util.Objects.requireNonNull;

/// An exact preimage or a proven superset. Conservative results require a residual predicate.
public record PreimageResult(Domain inputDomain, Exactness exactness)
{
    public PreimageResult
    {
        requireNonNull(inputDomain, "inputDomain is null");
        requireNonNull(exactness, "exactness is null");
    }

    public enum Exactness
    {
        EXACT,
        CONSERVATIVE,
    }
}
