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
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;
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

    PredicatePushdownController DISABLE_PUSHDOWN = (session, domain) -> new DomainPushdownResult(
            Domain.all(domain.getType()),
            domain);

    PredicatePushdownController CASE_INSENSITIVE_CHARACTER_PUSHDOWN = (session, domain) -> {
        checkArgument(
                domain.getType() instanceof VarcharType || domain.getType() instanceof CharType,
                "CASE_INSENSITIVE_CHARACTER_PUSHDOWN can be used only for chars and varchars");

        if (domain.isOnlyNull()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        if (!domain.getValues().isDiscreteSet()) {
            // case insensitive predicate pushdown could return incorrect results for operators like `!=`, `<` or `>`
            return DISABLE_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Domain#simplify can turn a discrete set into a range predicate
            // Push down of range predicate for varchar/char types could lead to incorrect results
            // when the remote database is case insensitive
            return DISABLE_PUSHDOWN.apply(session, domain);
        }
        return new DomainPushdownResult(simplifiedDomain, domain);
    };

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
