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
}
