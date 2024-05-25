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
package io.trino.plugin.accumulo.model;

import io.trino.spi.predicate.Domain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record AccumuloColumnConstraint(
        String name,
        String family,
        String qualifier,
        Optional<Domain> domain,
        boolean indexed)
{
    public AccumuloColumnConstraint
    {
        requireNonNull(name, "name is null");
        requireNonNull(family, "family is null");
        requireNonNull(qualifier, "qualifier is null");
        requireNonNull(domain, "domain is null");
    }
}
