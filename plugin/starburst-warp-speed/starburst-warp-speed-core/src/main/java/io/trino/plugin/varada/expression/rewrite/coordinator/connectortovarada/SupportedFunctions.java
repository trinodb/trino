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
package io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.StandardTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.varada.expression.rewrite.ExpressionService.PUSHDOWN_PREDICATES_STAT_GROUP;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.lang.String.format;

@Singleton
public class SupportedFunctions
{
    public static final FunctionName CEIL = new FunctionName("ceiling");
    public static final FunctionName IS_NAN = new FunctionName("is_nan");

    public static final FunctionName SUBSTRING = new FunctionName("substring");
    public static final FunctionName START_WITH = new FunctionName("starts_with");
    public static final FunctionName SUBSTR = new FunctionName("substr");
    public static final FunctionName STRPOS = new FunctionName("strpos");
    public static final FunctionName DAY = new FunctionName("day");
    public static final FunctionName DAY_OF_MONTH = new FunctionName("day_of_month"); //This is an alias for day().
    public static final FunctionName DAY_OF_WEEK = new FunctionName("day_of_week");
    public static final FunctionName DOW = new FunctionName("dow"); //This is an alias for day_of_week().
    public static final FunctionName DAY_OF_YEAR = new FunctionName("day_of_year");
    public static final FunctionName DOY = new FunctionName("doy"); //This is an alias for day_of_year().
    public static final FunctionName WEEK = new FunctionName("week"); //This is an alias for day_of_year().
    public static final FunctionName WEEK_OF_YEAR = new FunctionName("week_of_year"); //This is an alias for week()
    public static final FunctionName YEAR = new FunctionName("year"); //This is an alias for day_of_year().
    public static final FunctionName YEAR_OF_WEEK = new FunctionName("year_of_week"); //Returns the year of the ISO week from x.
    public static final FunctionName YOW = new FunctionName("yow"); //This is an alias for year_of_week().
    public static final FunctionName CONTAINS = new FunctionName("contains");

    public static final FunctionName ELEMENT_AT = new FunctionName("element_at");

    public static final FunctionName TRIM = new FunctionName("trim");
    public static final FunctionName LTRIM = new FunctionName("ltrim");
    public static final FunctionName RTRIM = new FunctionName("rtrim");

    public static final FunctionName SPLIT_PART = new FunctionName("split_part");

    public static final FunctionName LOWER = new FunctionName("lower");
    public static final FunctionName UPPER = new FunctionName("upper");

    public static final FunctionName JSON_EXTRACT_SCALAR = new FunctionName("json_extract_scalar");

    private final SetMultimap<FunctionName, ConnectorExpressionRule<Call, VaradaExpression>> supportedFunctionsRules = HashMultimap.create();
    private final VaradaStatsPushdownPredicates varadaStatsPushdownPredicates;

    public static final Set<FunctionName> DATE_FUNCTIONS = Set.of(SupportedFunctions.DOW,
            SupportedFunctions.DAY_OF_MONTH,
            SupportedFunctions.DAY,
            SupportedFunctions.DAY_OF_WEEK,
            SupportedFunctions.DAY_OF_YEAR,
            SupportedFunctions.DOY,
            SupportedFunctions.WEEK,
            SupportedFunctions.WEEK_OF_YEAR,
            SupportedFunctions.YOW,
            SupportedFunctions.YEAR_OF_WEEK);
    private final Set<String> comparableStandardFunctions = Set.of(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
            GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
            GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(),
            LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.getName(),
            LESS_THAN_OPERATOR_FUNCTION_NAME.getName());

    @Inject
    public SupportedFunctions(MetricsManager metricsManager)
    {
        this.varadaStatsPushdownPredicates = metricsManager.registerMetric(VaradaStatsPushdownPredicates.create(PUSHDOWN_PREDICATES_STAT_GROUP));

        Map<String, Set<String>> typeClasses = new HashMap<>();
        typeClasses.put("valid_types", Set.of(StandardTypes.REAL, StandardTypes.DOUBLE));
        Map<String, Set<String>> validDateTypes = new HashMap<>();
        validDateTypes.put("validDateTypes", Set.of(StandardTypes.DATE, StandardTypes.TIMESTAMP));

        supportedFunctionsRules.put(IN_PREDICATE_FUNCTION_NAME, new InRewriter());
        supportedFunctionsRules.put(GREATER_THAN_OPERATOR_FUNCTION_NAME, createGenericRewriter("$greater_than(x, value)", true));
        supportedFunctionsRules.put(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, createGenericRewriter("$greater_than_or_equal(x, value)", true));
        supportedFunctionsRules.put(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, createGenericRewriter("$less_than_or_equal(x ,value)", true));
        supportedFunctionsRules.put(LESS_THAN_OPERATOR_FUNCTION_NAME, createGenericRewriter("$less_than(x, value)", true));
        supportedFunctionsRules.put(NOT_EQUAL_OPERATOR_FUNCTION_NAME, createGenericRewriter("$not_equal(x, value)", true));
        supportedFunctionsRules.put(NOT_FUNCTION_NAME, new NotRewriter());
        supportedFunctionsRules.put(CAST_FUNCTION_NAME, new CastRewriter());
        supportedFunctionsRules.put(CONTAINS, new ContainsArrayRewriter());
        supportedFunctionsRules.put(OR_FUNCTION_NAME, new OrRewriter());
        supportedFunctionsRules.put(AND_FUNCTION_NAME, new AndRewriter());
        supportedFunctionsRules.put(ELEMENT_AT, createGenericRewriter("element_at(map : valid_types, key)", false,
                Map.of("valid_types", Set.of(StandardTypes.MAP))));
        supportedFunctionsRules.put(TRIM, createGenericRewriter("trim(string)", false));
        supportedFunctionsRules.put(RTRIM, createGenericRewriter("rtrim(string)", false));
        supportedFunctionsRules.put(LTRIM, createGenericRewriter("ltrim(string)", false));
        supportedFunctionsRules.put(SPLIT_PART, createGenericRewriter("split_part(string, delimiter, index)", false));
        supportedFunctionsRules.put(STRPOS, createGenericRewriter("strpos(string, substring, instance)", false));
        supportedFunctionsRules.put(STRPOS, createGenericRewriter("strpos(string, substring)", false));
        supportedFunctionsRules.put(SUBSTR, createGenericRewriter("substr(string, start)", false));
        supportedFunctionsRules.put(SUBSTR, createGenericRewriter("substr(string, start, length)", false));
        supportedFunctionsRules.put(SUBSTRING, createGenericRewriter("substring(string, start)", false));
        supportedFunctionsRules.put(SUBSTRING, createGenericRewriter("substring(string, start, length)", false));
        supportedFunctionsRules.put(EQUAL_OPERATOR_FUNCTION_NAME, createGenericRewriter("$equal(x, value)", true));
        supportedFunctionsRules.put(LIKE_FUNCTION_NAME, createGenericRewriter("$like(x ,pattern)", false));
        supportedFunctionsRules.put(START_WITH, createGenericRewriter("starts_with(string, substring)", false));
        supportedFunctionsRules.put(IS_NAN, createGenericRewriter("is_nan(x : valid_types)", false, typeClasses));
        supportedFunctionsRules.put(CEIL, createGenericRewriter("ceiling(x : valid_types)", false, typeClasses));
        supportedFunctionsRules.put(IS_NULL_FUNCTION_NAME, createGenericRewriter("$is_null(x)", false));
        supportedFunctionsRules.put(LOWER, createGenericRewriter("lower(x)", false, validDateTypes));
        supportedFunctionsRules.put(UPPER, createGenericRewriter("upper(x)", false, validDateTypes));
        supportedFunctionsRules.put(JSON_EXTRACT_SCALAR, createGenericRewriter("json_extract_scalar(varchar, jsonpath)", false));

        String structure = "%s(x : validDateTypes)";
        for (FunctionName dateFunction : DATE_FUNCTIONS) {
            String query = format(structure, dateFunction.getName());
            supportedFunctionsRules.put(dateFunction, createGenericRewriter(query, true, validDateTypes));
        }
    }

    public Set<ConnectorExpressionRule<Call, VaradaExpression>> getRule(FunctionName functionName)
    {
        return supportedFunctionsRules.get(functionName);
    }

    GenericRewriter createGenericRewriter(String expressionPattern, boolean allowCompositeExpression)
    {
        return createGenericRewriter(expressionPattern, allowCompositeExpression, Collections.emptyMap());
    }

    private GenericRewriter createGenericRewriter(String expressionPattern, boolean allowCompositeExpression, Map<String, Set<String>> typeClasses)
    {
        return new GenericRewriter(typeClasses, expressionPattern, allowCompositeExpression, varadaStatsPushdownPredicates);
    }

    @VisibleForTesting
    SetMultimap<FunctionName, ConnectorExpressionRule<Call, VaradaExpression>> getSupportedFunctionsRules()
    {
        return supportedFunctionsRules;
    }

    public Set<String> getComparableStandardFunctions()
    {
        return comparableStandardFunctions;
    }
}
