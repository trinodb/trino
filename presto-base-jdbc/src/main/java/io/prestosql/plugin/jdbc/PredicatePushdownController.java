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
package io.prestosql.plugin.jdbc;

import io.prestosql.spi.predicate.Domain;

import static java.util.Objects.requireNonNull;

public interface PredicatePushdownController
{
    DomainPushdownResult apply(Domain domain);

    final class DomainPushdownResult
    {
        private final Domain pushedDown;
        // In some cases, remainingFilter can be the same as pushedDown, e.g. when target database is case insensitive
        private final Domain remainingFilter;

        public DomainPushdownResult(Domain pushedDown, Domain remainingFilter)
        {
            this.pushedDown = requireNonNull(pushedDown, "pushedDown is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        }

        public Domain getPushedDown()
        {
            return pushedDown;
        }

        public Domain getRemainingFilter()
        {
            return remainingFilter;
        }
    }
}
