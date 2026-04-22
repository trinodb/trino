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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;

public class TupleDomainUtils
{
    private static final Logger log = Logger.get(TupleDomainUtils.class);

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

    /**
     * Evaluates whether a file's column statistics are compatible with the given predicate.
     * Returns true if the predicate <em>may</em> match (file should be read), false if the
     * statistics prove no row can satisfy the predicate (file can be skipped).
     */
    public static boolean evaluateStatisticPredicate(
            TupleDomain<String> regularColumnPredicates,
            Map<String, Domain> domainsWithStats,
            List<String> regularColumns)
    {
        if (regularColumnPredicates.isNone() || !regularColumnPredicates.getDomains().isPresent()) {
            return true;
        }
        for (String regularColumn : regularColumns) {
            Domain columnPredicate = regularColumnPredicates.getDomains().get().get(regularColumn);
            Optional<Domain> currentColumnStats = Optional.ofNullable(domainsWithStats.get(regularColumn));
            if (currentColumnStats.isEmpty()) {
                // No stats for column
            }
            else {
                Domain domain = currentColumnStats.get();
                if (columnPredicate.intersect(domain).isNone()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Constructs a {@link Domain} from a column's min/max statistics values.
     * Returns {@link Domain#all} for unsupported types or when statistics are out of range.
     */
    public static Domain getDomainFromColumnStats(String colName, Type type, Object minimum, Object maximum, boolean hasNullValue)
    {
        try {
            if (type.equals(BOOLEAN)) {
                boolean hasTrueValue = (boolean) minimum || (boolean) maximum;
                boolean hasFalseValue = !(boolean) minimum || !(boolean) maximum;
                if (hasTrueValue && hasFalseValue) {
                    return Domain.all(type);
                }
                if (hasTrueValue) {
                    return Domain.create(ValueSet.of(type, true), hasNullValue);
                }
                if (hasFalseValue) {
                    return Domain.create(ValueSet.of(type, false), hasNullValue);
                }
                // No other case, since all null case is handled earlier.
            }

            if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT)
                    || type.equals(INTEGER) || type.equals(DATE))) {
                long minValue = TupleDomainParquetPredicate.asLong(minimum);
                long maxValue = TupleDomainParquetPredicate.asLong(maximum);
                if (isStatisticsOverflow(type, minValue, maxValue)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (type.equals(REAL)) {
                Float minValue = (Float) minimum;
                Float maxValue = (Float) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, (long) floatToRawIntBits(minValue), (long) floatToRawIntBits(maxValue), hasNullValue);
            }

            if (type.equals(DOUBLE)) {
                Double minValue = (Double) minimum;
                Double maxValue = (Double) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (type.equals(VarcharType.VARCHAR)) {
                Slice min = Slices.utf8Slice((String) minimum);
                Slice max = Slices.utf8Slice((String) maximum);
                return ofMinMax(type, min, max, hasNullValue);
            }
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        catch (Exception e) {
            log.warn("failed to create Domain for column: %s which type is: %s", colName, type.toString());
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }

    private static Domain ofMinMax(Type type, Object min, Object max, boolean hasNullValue)
    {
        Range range = Range.range(type, min, true, max, true);
        ValueSet vs = ValueSet.ofRanges(ImmutableList.of(range));
        return Domain.create(vs, hasNullValue);
    }

    public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";
    public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";

    /**
     * Extracts predicates from a TupleDomain that match a given set of columns.
     * Preserves all complex predicate properties including multi-value domains,
     * range-based predicates, and nullability.
     *
     * @param tupleDomain The source TupleDomain containing all predicates
     * @param columnFields The set of columns for which to extract predicates
     * @return A new TupleDomain containing only the predicates for the specified columns
     */
    public static TupleDomain<String> extractPredicatesForColumns(TupleDomain<String> tupleDomain, List<String> columnFields)
    {
        if (tupleDomain.isNone()) {
            return TupleDomain.none();
        }

        if (tupleDomain.isAll()) {
            return TupleDomain.all();
        }

        // Extract the domains matching the specified columns
        Map<String, Domain> allDomains = tupleDomain.getDomains().get();
        Map<String, Domain> filteredDomains = allDomains.entrySet().stream().filter(entry -> columnFields.contains(entry.getKey())) // Ensure key is in the column set
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // If no domains matched, but we had some columns to extract, return ALL
        if (filteredDomains.isEmpty() && !columnFields.isEmpty() && !allDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(filteredDomains);
    }

    /**
     * Constructs a record key from TupleDomain based on whether it's a complex key or not.
     * <p>
     * Construction of record keys will only be handled for domains generated from EQUALITY or IN predicates.
     * <p>
     * An empty list of record keys will be generated if the following conditions are not met:
     * <ol>
     * <li>recordKeysFields is empty</li>
     * <li>recordKeyDomains isAll</li>
     * <li>For the case of complex key, domains are not applied to all recordKeysFields</li>
     * <li>For the case of complex key, domains are applied to all recordKeyFields, but one of the domain is <b>NOT</b>
     * generated from an equality or IN predicate</li>
     * </ol>
     * <p>
     * Note: This function is O(m^n) where m is the average size of value literals and n is the number of record keys.
     * <p>
     * Optimization 1: If MDT enabled functions allows for streams to be passed in, we can implement an iterator to be more memory efficient.
     * <p>
     * Optimization 2: We should also consider limiting the number of recordKeys generated, if it is estimated to be more than a limit, RLI should just be skipped
     * as it may just be faster to read out all data and filer accordingly.
     *
     * For a composite key, each part is encoded as {@code field<DEFAULT_COLUMN_VALUE_SEPARATOR>value},
     * and parts are joined with {@code DEFAULT_RECORD_KEY_PARTS_SEPARATOR}. For example, a composite key
     * on fields {@code (a, b)} with values {@code (1, 2)} produces {@code "a:1,b:2"}.
     *
     * @param recordKeyDomains The filtered TupleDomain containing column handles and values
     * @param recordKeyFields List of column names that represent the record keys
     * @return List of string values representing the record key(s)
     */
    public static List<String> constructRecordKeys(TupleDomain<String> recordKeyDomains, List<String> recordKeyFields)
    {
        // If no recordKeys or no recordKeyDomains, return empty list
        if (recordKeyFields == null || recordKeyFields.isEmpty() || recordKeyDomains.isAll()) {
            return Collections.emptyList();
        }

        // All recordKeys must have a domain else, return empty list (applicable to complexKeys)
        // If a one of the recordKey in the set of complexKeys does not have a domain, we are unable to construct
        // a complete complexKey
        if (!recordKeyDomains.getDomains().get().keySet().containsAll(recordKeyFields)) {
            return Collections.emptyList();
        }

        // Extract the domain mappings from the tuple domain
        Map<String, Domain> domains = recordKeyDomains.getDomains().get();

        // Case 1: Not a complex key (single record key)
        if (recordKeyFields.size() == 1) {
            String recordKey = recordKeyFields.getFirst();

            // Extract value for this key
            Domain domain = domains.get(recordKey);
            return extractStringValues(domain);
        }
        // Case 2: Complex/Composite key (multiple record keys)
        else {
            // Create a queue to manage the Cartesian product generation
            Queue<String> results = new LinkedList<>();

            // For each key in the complex key
            for (String recordKeyField : recordKeyFields) {
                // Extract value for this key
                Domain domain = domains.get(recordKeyField);
                List<String> values = extractStringValues(domain);
                // First iteration: initialize the queue
                if (results.isEmpty()) {
                    values.forEach(v -> results.offer(recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v));
                }
                else {
                    int size = results.size();
                    for (int j = 0; j < size; j++) {
                        String currentEntry = results.poll();

                        // Generate new combinations by appending keyParts to existing keyParts
                        for (String v : values) {
                            String newKeyPart = recordKeyField + DEFAULT_COLUMN_VALUE_SEPARATOR + v;
                            String newEntry = currentEntry + DEFAULT_RECORD_KEY_PARTS_SEPARATOR + newKeyPart;
                            results.offer(newEntry);
                        }
                    }
                }
            }
            return results.stream().toList();
        }
    }

    /**
     * Extract string values from a domain, handle EQUAL and IN domains only.
     */
    private static List<String> extractStringValues(Domain domain)
    {
        List<String> values = new ArrayList<>();

        if (domain.isSingleValue()) {
            // Handle EQUAL condition (single value domain)
            Object value = domain.getSingleValue();
            values.add(convertToString(value));
        }
        else if (domain.getValues().isDiscreteSet()) {
            // Handle IN condition (set of discrete values)
            for (Object value : domain.getValues().getDiscreteSet()) {
                values.add(convertToString(value));
            }
        }
        return values;
    }

    private static String convertToString(Object value)
    {
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        else {
            return value.toString();
        }
    }
}
