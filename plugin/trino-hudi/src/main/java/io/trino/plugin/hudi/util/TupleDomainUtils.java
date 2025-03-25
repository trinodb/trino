package io.trino.plugin.hudi.util;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TupleDomainUtils {

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
    public static boolean areSomeFieldsReferenced(TupleDomain<String> tupleDomain, List<String> sourceFields) {
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
        if (sourceFields == null || sourceFields.isEmpty() || tupleDomain.isAll()) {
            return false;
        }

        boolean areReferencedInOrEqual = true;
        for (String sourceField : sourceFields) {
            Domain domain = tupleDomain.getDomains().get().get(sourceField);
            // Check if equals
            areReferencedInOrEqual &= (domain.isSingleValue() || domain.getValues().isDiscreteSet());
        }
        return areReferencedInOrEqual;
    }
}
