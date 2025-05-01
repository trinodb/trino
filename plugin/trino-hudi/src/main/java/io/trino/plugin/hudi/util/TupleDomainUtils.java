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
package io.trino.plugin.hudi.util;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TupleDomainUtils
{
    // Utility classes should not have a public or default constructor.
    private TupleDomainUtils() {}

    /**
     * Get all columns that are referenced in the provided tupleDomain predicates.
     */
    public static List<String> getReferencedColumns(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return List.of();
        }
        return tupleDomain.getDomains().get().keySet().stream().toList();
    }

    /**
     * Check if all of the provided source fields are referenced in the tupleDomain predicates.
     */
    public static boolean areAllFieldsReferenced(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        Set<String> referenceColSet = new HashSet<>(TupleDomainUtils.getReferencedColumns(tupleDomain));
        Set<String> sourceFieldSet = new HashSet<>(sourceFields);

        return referenceColSet.containsAll(sourceFieldSet);
    }

    /**
     * Check if at least one of the provided source field is referenced in the tupleDomain predicates.
     */
    public static boolean areSomeFieldsReferenced(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        Set<String> referenceColSet = new HashSet<>(TupleDomainUtils.getReferencedColumns(tupleDomain));
        for (String sourceField : sourceFields) {
            if (referenceColSet.contains(sourceField)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check all columns referencing sourceFields are either IN or EQUAL predicates.
     */
    public static boolean areDomainsInOrEqualOnly(TupleDomain<String> tupleDomain, List<String> sourceFields)
    {
        // If no recordKeys or no recordKeyDomains, return empty list
        if (sourceFields == null || sourceFields.isEmpty() || tupleDomain.isAll() || tupleDomain.isNone()) {
            return false;
        }

        Optional<Map<String, Domain>> domainsOpt = tupleDomain.getDomains();
        // Not really necessary, as tupleDomain.isNone() already checks for this
        if (domainsOpt.isEmpty()) {
            return false;
        }

        boolean areReferencedInOrEqual = true;
        for (String sourceField : sourceFields) {
            Domain domain = domainsOpt.get().get(sourceField);
            // For cases where sourceField does not exist in tupleDomain
            if (domain == null) {
                return false;
            }
            areReferencedInOrEqual &= (domain.isSingleValue() || domain.getValues().isDiscreteSet());
        }
        return areReferencedInOrEqual;
    }

    /**
     * Checks if a specific Domain represents ONLY an 'IS NULL' constraint.
     * This means null is allowed, and no other non-null values are allowed.
     * Important: Not handling `= NULL` predicates as colA `= NULL` does not evaluate to TRUE or FALSE, it evaluates to UNKNOWN, which is treated as false.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NULL', false otherwise.
     */
    private static boolean isOnlyNullConstraint(Domain domain)
    {
        // Null must be allowed, and the ValueSet must allow *no* non-null values.
        return domain.isNullAllowed() && domain.getValues().isNone();
    }

    /**
     * Checks if a specific Domain represents ONLY an 'IS NOT NULL' constraint.
     * This means null is not allowed, and all non-null values are allowed (no other range/value restrictions).
     * Important: Not handling `!= NULL` or `<> NULL` predicates as this does not evaluate to TRUE or FALSE, it evaluates to UNKNOWN, which is treated as false.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NOT NULL', false otherwise.
     */
    private static boolean isOnlyNotNullConstraint(Domain domain)
    {
        // Null must *NOT* be allowed, and the ValueSet must allow *ALL* possible non-null values.
        return !domain.isNullAllowed() && domain.getValues().isAll();
    }

    /**
     * Overloaded function to test if a Domain contains null checks or not.
     *
     * @param domain The Domain to check.
     * @return true if the domain represents 'IS NOT NULL' or 'IS NULL', false otherwise.
     */
    public static boolean hasSimpleNullCheck(Domain domain)
    {
        return isOnlyNullConstraint(domain) || isOnlyNotNullConstraint(domain);
    }

    /**
     * Checks if a TupleDomain contains at least one column Domain that represents
     * exclusively an 'IS NULL' or 'IS NOT NULL' constraint.
     *
     * @param tupleDomain The TupleDomain to inspect.
     * @return true if a simple null check constraint exists, false otherwise.
     */
    public static boolean hasSimpleNullCheck(TupleDomain<String> tupleDomain)
    {
        // A 'None' TupleDomain implies contradiction, not a simple null check
        if (tupleDomain.isNone()) {
            return false;
        }
        Optional<Map<String, Domain>> domains = tupleDomain.getDomains();
        // An 'All' TupleDomain has no constraints
        if (domains.isEmpty()) {
            return false;
        }

        // Iterate through the domains for each column in the TupleDomain
        for (Domain domain : domains.get().values()) {
            if (hasSimpleNullCheck(domain)) {
                // Found a domain that is purely an IS NULL or IS NOT NULL check
                return true;
            }
        }
        // No domain matched the simple null check patterns
        return false;
    }
}
