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
package io.trino.plugin.pinot.query;

import io.trino.matching.Pattern;
import io.trino.matching.Property;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import java.util.List;
import java.util.Optional;

import static io.trino.matching.Pattern.typeOf;
import static io.trino.plugin.pinot.query.PinotTransformFunctionTypeResolver.getTransformFunctionType;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.FUNCTION;
import static org.apache.pinot.common.request.context.ExpressionContext.Type.IDENTIFIER;
import static org.apache.pinot.common.request.context.FunctionContext.Type.AGGREGATION;
import static org.apache.pinot.common.request.context.FunctionContext.Type.TRANSFORM;
import static org.apache.pinot.common.request.context.predicate.RangePredicate.UNBOUNDED;
import static org.apache.pinot.segment.spi.AggregationFunctionType.getAggregationFunctionType;

public class PinotPatterns
{
    public static final String WILDCARD = "*";

    private PinotPatterns() {}

    public static Pattern<FilterContext> filter()
    {
        return typeOf(FilterContext.class);
    }

    public static Pattern<Predicate> predicate()
    {
        return typeOf(Predicate.class);
    }

    public static Pattern<ExpressionContext> expression()
    {
        return typeOf(ExpressionContext.class);
    }

    public static Pattern<FunctionContext> function()
    {
        return typeOf(FunctionContext.class);
    }

    public static Pattern<FunctionContext> transformFunction()
    {
        return function()
                .with(functionType().equalTo(TRANSFORM));
    }

    public static Pattern<FunctionContext> aggregationFunction()
    {
        return function()
                .with(functionType().equalTo(AGGREGATION));
    }

    public static Pattern<FunctionContext> binaryFunction()
    {
        return transformFunction()
                .with(arguments().matching(arguments -> arguments.size() == 2));
    }

    // Filter Properties
    public static Property<FilterContext, ?, FilterContext.Type> filterType()
    {
        return Property.property("filterContextType", FilterContext::getType);
    }

    public static Property<FilterContext, ?, List<FilterContext>> childFilters()
    {
        return Property.optionalProperty("childFilters", context -> {
            if (context.getType() == FilterContext.Type.AND || context.getType() == FilterContext.Type.OR) {
                return Optional.ofNullable(context.getChildren());
            }
            return Optional.empty();
        });
    }

    public static Property<FilterContext, ?, Predicate> filterPredicate()
    {
        return Property.optionalProperty("filterPredicate", context -> {
            if (context.getType() == FilterContext.Type.PREDICATE) {
                return Optional.ofNullable(context.getPredicate());
            }
            return Optional.empty();
        });
    }

    // Predicate Properties
    public static Property<Predicate, ?, Predicate.Type> predicateType()
    {
        return Property.property("predicateType", Predicate::getType);
    }

    public static Property<Predicate, ?, ExpressionContext> predicateExpression()
    {
        return Property.property("predicateType", Predicate::getLhs);
    }

    public static Property<Predicate, ?, String> binaryOperatorValue()
    {
        return Property.optionalProperty("binaryOperatorValue", predicate -> {
            switch (predicate.getType()) {
                case EQ:
                    return Optional.of(((EqPredicate) predicate).getValue());
                case NOT_EQ:
                    return Optional.of(((NotEqPredicate) predicate).getValue());
                case RANGE:
                    RangePredicate rangePredicate = (RangePredicate) predicate;
                    if (rangePredicate.getLowerBound().equals(UNBOUNDED)) {
                        return Optional.of(rangePredicate.getUpperBound());
                    }
                    if (rangePredicate.getUpperBound().equals(UNBOUNDED)) {
                        return Optional.of(rangePredicate.getLowerBound());
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        });
    }

    public static Property<Predicate, ?, String> binaryOperator()
    {
        return Property.optionalProperty("binaryOperator", predicate -> {
            switch (predicate.getType()) {
                case EQ:
                    return Optional.of("=");
                case NOT_EQ:
                    return Optional.of("!=");
                case RANGE:
                    RangePredicate rangePredicate = (RangePredicate) predicate;
                    if (rangePredicate.getLowerBound().equals(UNBOUNDED)) {
                        if (rangePredicate.isUpperInclusive()) {
                            return Optional.of("<=");
                        }
                        return Optional.of("<");
                    }
                    if (rangePredicate.getUpperBound().equals(UNBOUNDED)) {
                        if (rangePredicate.isLowerInclusive()) {
                            return Optional.of(">=");
                        }
                        return Optional.of(">");
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        });
    }

    public static Property<Predicate, ?, List<String>> predicateValuesList()
    {
        return Property.optionalProperty("predicateValuesList", predicate -> {
            if (predicate.getType() == Predicate.Type.IN) {
                return Optional.of(((InPredicate) predicate).getValues());
            }
            if (predicate.getType() == Predicate.Type.NOT_IN) {
                return Optional.of(((NotInPredicate) predicate).getValues());
            }
            return Optional.empty();
        });
    }

    public static Property<Predicate, ?, String> binaryFunctionPredicateValue()
    {
        return Property.optionalProperty("binaryFunctionPredicateValue", predicate -> {
            switch (predicate.getType()) {
                case REGEXP_LIKE:
                    return Optional.of(((RegexpLikePredicate) predicate).getValue());
                case TEXT_MATCH:
                    return Optional.of(((TextMatchPredicate) predicate).getValue());
                case JSON_MATCH:
                    return Optional.of(((JsonMatchPredicate) predicate).getValue());
                default:
                    return Optional.empty();
            }
        });
    }

    // Expression Properties
    public static Property<ExpressionContext, ?, FunctionContext> functionContext()
    {
        return Property.optionalProperty("functionContext", expressionContext -> {
            if (expressionContext.getType() == FUNCTION) {
                return Optional.of(expressionContext.getFunction());
            }
            return Optional.empty();
        });
    }

    public static Property<ExpressionContext, ?, ExpressionContext.Type> expressionType()
    {
        return Property.property("expressionType", ExpressionContext::getType);
    }

    public static Property<ExpressionContext, ?, String> identifier()
    {
        return Property.optionalProperty("identifier", expressionContext -> {
            if (expressionContext.getType() == IDENTIFIER) {
                return Optional.of(expressionContext.getIdentifier());
            }
            return Optional.empty();
        });
    }

    // Function Properties
    public static Property<FunctionContext, ?, TransformFunctionType> transformFunctionType()
    {
        return Property.optionalProperty("transformFunctionType", functionContext -> {
            if (functionContext.getType() == TRANSFORM) {
                return getTransformFunctionType(functionContext);
            }
            return Optional.empty();
        });
    }

    public static Property<FunctionContext, ?, String> transformFunctionName()
    {
        return Property.optionalProperty("transformFunctionType", functionContext -> {
            if (functionContext.getType() == TRANSFORM) {
                return Optional.of(functionContext.getFunctionName());
            }
            return Optional.empty();
        });
    }

    // AggregationFunction Properties
    public static Property<FunctionContext, ?, AggregationFunctionType> aggregationFunctionType()
    {
        return Property.optionalProperty("aggregationFunctionType", functionContext -> {
            if (functionContext.getType() == AGGREGATION) {
                return Optional.of(getAggregationFunctionType(functionContext.getFunctionName()));
            }
            return Optional.empty();
        });
    }

    public static Property<FunctionContext, ?, FunctionContext.Type> functionType()
    {
        return Property.property("functionType", FunctionContext::getType);
    }

    public static Property<FunctionContext, ?, List<ExpressionContext>> arguments()
    {
        return Property.property("arguments", FunctionContext::getArguments);
    }

    public static Property<FunctionContext, ?, ExpressionContext> singleInput()
    {
        return Property.optionalProperty("singleInput", functionContext -> {
            if (functionContext.getArguments().size() == 1) {
                return Optional.of(functionContext.getArguments().get(0));
            }
            return Optional.empty();
        });
    }

    public static Property<FunctionContext, ?, ExpressionContext> firstArgument()
    {
        return Property.optionalProperty("firstArgument", functionContext -> {
            if (!functionContext.getArguments().isEmpty()) {
                return Optional.of(functionContext.getArguments().get(0));
            }
            return Optional.empty();
        });
    }

    public static Property<FunctionContext, ?, ExpressionContext> secondArgument()
    {
        return Property.optionalProperty("secondArgument", functionContext -> {
            if (functionContext.getArguments().size() > 1) {
                return Optional.of(functionContext.getArguments().get(1));
            }
            return Optional.empty();
        });
    }
}
