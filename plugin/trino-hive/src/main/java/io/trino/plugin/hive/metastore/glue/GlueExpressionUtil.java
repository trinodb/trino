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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class GlueExpressionUtil
{
    static final String NULL_STRING = "__HIVE_DEFAULT_PARTITION__";
    static final int GLUE_EXPRESSION_CHAR_LIMIT = 2048;

    private static final Set<String> QUOTED_TYPES = ImmutableSet.of("string", "char", "varchar", "date", "timestamp", "binary", "varbinary");
    private static final String CONJUNCT_SEPARATOR = " AND ";
    private static final Joiner CONJUNCT_JOINER = Joiner.on(CONJUNCT_SEPARATOR);
    private static final String DISJUNCT_SEPARATOR = " OR ";
    private static final Joiner DISJUNCT_JOINER = Joiner.on(DISJUNCT_SEPARATOR);

    /**
     * AWS Glue uses internally <a href="http://jsqlparser.sourceforge.net/home.php">JSQLParser</a>
     * as SQL statement parser for the expression that can be used to filter the partitions.
     * JSQLParser defines a set of reserved keywords which cannot be used as column names in the filter.
     *
     * @see <a href="https://sourceforge.net/p/jsqlparser/code/HEAD/tree/trunk/src/main/javacc/JSqlParserCC.jj">JSqlParser Grammar</a>
     */
    private static final Set<String> JSQL_PARSER_RESERVED_KEYWORDS = ImmutableSet.of(
            "AS", "BY", "DO", "IS", "IN", "OR", "ON", "ALL", "AND", "ANY", "KEY", "NOT", "SET", "ASC", "TOP", "END", "DESC", "INTO", "NULL", "LIKE", "DROP", "JOIN",
            "LEFT", "FROM", "OPEN", "CASE", "WHEN", "THEN", "ELSE", "SOME", "FULL", "WITH", "TABLE", "WHERE", "USING", "UNION", "GROUP", "BEGIN", "INDEX", "INNER",
            "LIMIT", "OUTER", "ORDER", "RIGHT", "DELETE", "CREATE", "SELECT", "OFFSET", "EXISTS", "HAVING", "INSERT", "UPDATE", "VALUES", "ESCAPE", "PRIMARY",
            "NATURAL", "REPLACE", "BETWEEN", "TRUNCATE", "DISTINCT", "INTERSECT");

    private GlueExpressionUtil() {}

    private static boolean isQuotedType(Type type)
    {
        return QUOTED_TYPES.contains(type.getTypeSignature().getBase());
    }

    private static String valueToString(Type type, Object value)
    {
        String s = MetastoreUtil.sqlScalarToString(type, value, NULL_STRING);

        return isQuotedType(type) ? "'" + s + "'" : s;
    }

    private static boolean canConvertSqlTypeToStringForGlue(Type type, boolean assumeCanonicalPartitionKeys)
    {
        return !(type instanceof TimestampType) && !(type instanceof DateType) && (type instanceof CharType || type instanceof VarcharType || assumeCanonicalPartitionKeys);
    }

    /**
     * @return a valid glue expression <= {@link  GlueExpressionUtil#GLUE_EXPRESSION_CHAR_LIMIT}. A return value of "" means a valid glue expression could not be created, or
     * {@link TupleDomain#all()} was passed in as an argument
     */
    public static String buildGlueExpression(List<String> columnNames, TupleDomain<String> partitionKeysFilter, boolean assumeCanonicalPartitionKeys)
    {
        return buildGlueExpression(columnNames, partitionKeysFilter, assumeCanonicalPartitionKeys, GLUE_EXPRESSION_CHAR_LIMIT);
    }

    public static String buildGlueExpression(List<String> columnNames, TupleDomain<String> partitionKeysFilter, boolean assumeCanonicalPartitionKeys, int expressionLengthLimit)
    {
        // this should have been handled by callers
        checkState(!partitionKeysFilter.isNone());
        if (partitionKeysFilter.isAll()) {
            // glue handles both null and "" as a tautology
            return "";
        }

        List<String> perColumnExpressions = new ArrayList<>();
        int expressionLength = 0;
        Map<String, Domain> domains = partitionKeysFilter.getDomains().get();
        for (String columnName : columnNames) {
            if (JSQL_PARSER_RESERVED_KEYWORDS.contains(columnName.toUpperCase(ENGLISH))) {
                // The column name is a reserved keyword in the grammar of the SQL parser used internally by Glue API
                continue;
            }
            Domain domain = domains.get(columnName);
            if (domain != null) {
                Optional<String> columnExpression = buildGlueExpressionForSingleDomain(columnName, domain, assumeCanonicalPartitionKeys);
                if (columnExpression.isPresent()) {
                    int newExpressionLength = expressionLength;
                    if (expressionLength > 0) {
                        newExpressionLength += CONJUNCT_SEPARATOR.length();
                    }

                    newExpressionLength += columnExpression.get().length();

                    if (newExpressionLength > expressionLengthLimit) {
                        continue;
                    }

                    perColumnExpressions.add(columnExpression.get());
                    expressionLength = newExpressionLength;
                }
            }
        }

        return Joiner.on(CONJUNCT_SEPARATOR).join(perColumnExpressions);
    }

    /**
     * Converts domain to a valid glue expression per
     * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
     *
     * @return optional glue-compatible expression. if empty, either tye type of the {@link Domain} cannot be converted to a string, or the filter is {@link Domain#all}
     */
    @VisibleForTesting
    static Optional<String> buildGlueExpressionForSingleDomain(String columnName, Domain domain, boolean assumeCanonicalPartitionKeys)
    {
        ValueSet valueSet = domain.getValues();

        if (domain.isAll() || !canConvertSqlTypeToStringForGlue(domain.getType(), assumeCanonicalPartitionKeys)) {
            // if the type can't be converted or Domain.all()
            return Optional.empty();
        }

        if (domain.getValues().isAll()) {
            return Optional.of(format("(%s <> '%s')", columnName, NULL_STRING));
        }

        // null must be allowed for this case since callers must filter Domain.none() out
        if (domain.getValues().isNone()) {
            return Optional.of(format("(%s = '%s')", columnName, NULL_STRING));
        }

        List<String> disjuncts = new ArrayList<>();
        List<String> singleValues = new ArrayList<>();
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(valueToString(range.getType(), range.getSingleValue()));
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(format(
                            "%s %s %s",
                            columnName,
                            range.isLowInclusive() ? ">=" : ">",
                            valueToString(range.getType(), range.getLowBoundedValue())));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(format(
                            "%s %s %s",
                            columnName,
                            range.isHighInclusive() ? "<=" : "<",
                            valueToString(range.getType(), range.getHighBoundedValue())));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for by callers
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + CONJUNCT_JOINER.join(rangeConjuncts) + ")");
            }
        }

        if (singleValues.size() == 1) {
            String equalsTest = format("(%s = %s)", columnName, getOnlyElement(singleValues));
            disjuncts.add(equalsTest);
        }
        else if (singleValues.size() > 1) {
            String values = Joiner.on(", ").join(singleValues);
            String inClause = format("(%s in (%s))", columnName, values);

            disjuncts.add(inClause);
        }

        return Optional.of("(" + DISJUNCT_JOINER.join(disjuncts) + ")");
    }
}
