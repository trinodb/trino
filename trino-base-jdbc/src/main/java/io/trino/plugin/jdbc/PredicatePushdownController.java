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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;

import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static java.util.Objects.requireNonNull;

public interface PredicatePushdownController
{
    PredicatePushdownController FULL_PUSHDOWN = (session, domain) -> {
        if (getDomainSize(domain) > getDomainCompactionThreshold(session)) {
            // pushdown simplified domain
            return new DomainPushdownResult(domain.simplify(getDomainCompactionThreshold(session)), domain);
        }
        return new DomainPushdownResult(domain, Domain.all(domain.getType()));
    };
    PredicatePushdownController PUSHDOWN_AND_KEEP = (session, domain) -> new DomainPushdownResult(
            domain.simplify(getDomainCompactionThreshold(session)),
            domain);
    PredicatePushdownController DISABLE_PUSHDOWN = (session, domain) -> new DomainPushdownResult(
            Domain.all(domain.getType()),
            domain);

    DomainPushdownResult apply(ConnectorSession session, Domain domain);

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

    private static int getDomainSize(Domain domain)
    {
        return domain.getValues().getValuesProcessor().transform(
                Ranges::getRangeCount,
                DiscreteValues::getValuesCount,
                ignored -> 0);
    }
}
