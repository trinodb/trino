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
package io.trino.plugin.varada.warmup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import io.trino.plugin.varada.api.warmup.column.TransformedColumnData;
import io.trino.plugin.varada.api.warmup.column.VaradaColumnData;
import io.trino.plugin.varada.api.warmup.column.WildcardColumnData;
import io.trino.plugin.varada.api.warmup.expression.TransformFunctionData;
import io.trino.plugin.varada.api.warmup.expression.VaradaConstantData;
import io.trino.plugin.varada.api.warmup.expression.VaradaExpressionData;
import io.trino.plugin.varada.api.warmup.expression.VaradaPrimitiveConstantData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.warmup.model.DateRangeSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.TrinoException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class WarmupRuleApiMapper
{
    private WarmupRuleApiMapper() {}

    public static WarmupRule toModel(WarmupColRuleData warmupColRuleData)
    {
        Set<WarmupPredicateRule> predicates = warmupColRuleData.getPredicates().stream().map(WarmupRuleApiMapper::convertFromApiPredicate).collect(Collectors.toSet());
        WarmUpType warmUpType = convertApiWarmUpType(warmupColRuleData.getWarmUpType());
        return WarmupRule.builder()
                .id(warmupColRuleData.getId())
                .warmUpType(warmUpType)
                .table(warmupColRuleData.getTable())
                .schema(warmupColRuleData.getSchema())
                .predicates(predicates)
                .ttl((int) warmupColRuleData.getTtl().getSeconds())
                .priority(warmupColRuleData.getPriority())
                .varadaColumn(createVaradaColumn(warmupColRuleData))
                .build();
    }

    private static VaradaColumn createVaradaColumn(WarmupColRuleData warmupColRuleData)
    {
        if (warmupColRuleData.getColumn() instanceof TransformedColumnData transformedColumnData) {
            TransformFunction transformFunction = convertApiTransformFunction(transformedColumnData.getTransformFunction());
            return new TransformedColumn(transformedColumnData.getKey(), transformedColumnData.getKey(), transformFunction);
        }
        else if (warmupColRuleData.getColumn() instanceof RegularColumnData) {
            return new RegularColumn(warmupColRuleData.getColumn().getKey());
        }
        else if (warmupColRuleData.getColumn() instanceof WildcardColumnData) {
            return new WildcardColumn();
        }
        throw new IllegalArgumentException("Could not create a VaradaColumn " + warmupColRuleData.getColumn());
    }

    private static WarmupPredicateRule convertFromApiPredicate(io.trino.plugin.varada.api.warmup.WarmupPredicateRule apiWarmupPredicateRule)
    {
        if (apiWarmupPredicateRule instanceof io.trino.plugin.varada.api.warmup.PartitionValueWarmupPredicateRule apiValuePredicate) {
            return new PartitionValueWarmupPredicateRule(apiValuePredicate.getColumnId(), apiValuePredicate.getValue());
        }
        else if (apiWarmupPredicateRule instanceof io.trino.plugin.varada.api.warmup.DateRangeSlidingWindowWarmupPredicateRule apiRangePredicate) {
            return new DateRangeSlidingWindowWarmupPredicateRule(apiRangePredicate.getColumnId(), apiRangePredicate.getStartRangeDaysBefore(), apiRangePredicate.getEndRangeDaysBefore(), apiRangePredicate.getWindowDateFormat());
        }
        else if (apiWarmupPredicateRule instanceof io.trino.plugin.varada.api.warmup.DateSlidingWindowWarmupPredicateRule apiSlidingPredicate) {
            return new DateSlidingWindowWarmupPredicateRule(apiSlidingPredicate.getColumnId(), apiSlidingPredicate.getWindowSizeDays(), apiSlidingPredicate.getWindowDateFormat(), apiSlidingPredicate.getBaseDate());
        }
        return null;
    }

    private static WarmUpType convertApiWarmUpType(io.trino.plugin.varada.api.warmup.WarmUpType apiWarmUpType)
    {
        return switch (apiWarmUpType) {
            case WARM_UP_TYPE_BASIC -> WarmUpType.WARM_UP_TYPE_BASIC;
            case WARM_UP_TYPE_DATA -> WarmUpType.WARM_UP_TYPE_DATA;
            case WARM_UP_TYPE_LUCENE -> WarmUpType.WARM_UP_TYPE_LUCENE;
            case WARM_UP_TYPE_BLOOM_HIGH -> WarmUpType.WARM_UP_TYPE_BLOOM_HIGH;
            case WARM_UP_TYPE_BLOOM_MEDIUM -> WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM;
            case WARM_UP_TYPE_BLOOM_LOW -> WarmUpType.WARM_UP_TYPE_BLOOM_LOW;
        };
    }

    private static TransformFunction.TransformType convertApiTransformType(TransformFunctionData.TransformType apiTransformType)
    {
        return switch (apiTransformType) {
            case LOWER -> TransformFunction.TransformType.LOWER;
            case UPPER -> TransformFunction.TransformType.UPPER;
            case DATE -> TransformFunction.TransformType.DATE;
            case ELEMENT_AT -> TransformFunction.TransformType.ELEMENT_AT;
            case JSON_EXTRACT_SCALAR -> TransformFunction.TransformType.JSON_EXTRACT_SCALAR;
            default -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Unknown apiTransformType " + apiTransformType);
        };
    }

    private static VaradaPrimitiveConstant convertApiVaradaPrimitiveConstant(VaradaPrimitiveConstantData apiVaradaPrimitiveConstant)
    {
        Object value = apiVaradaPrimitiveConstant.getValue();
        VaradaExpressionData.Type type = apiVaradaPrimitiveConstant.getType();

        switch (type) {
            case VARCHAR -> {
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(value, VarcharType.VARCHAR);
                }
            }
            case INTEGER -> {
                if (value instanceof Integer) {
                    return new VaradaPrimitiveConstant(value, IntegerType.INTEGER);
                }
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(Integer.valueOf((String) value), IntegerType.INTEGER);
                }
            }
            case BIGINT -> {
                if (value instanceof Integer) {
                    return new VaradaPrimitiveConstant(((Integer) value).longValue(), BigintType.BIGINT);
                }
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(Long.valueOf((String) value), BigintType.BIGINT);
                }
            }
            case SMALLINT -> {
                if (value instanceof Integer) {
                    return new VaradaPrimitiveConstant(((Integer) value).shortValue(), SmallintType.SMALLINT);
                }
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(Short.valueOf((String) value), SmallintType.SMALLINT);
                }
            }
            case DOUBLE -> {
                if (value instanceof Double) {
                    return new VaradaPrimitiveConstant(value, DoubleType.DOUBLE);
                }
                if (value instanceof Integer) {
                    return new VaradaPrimitiveConstant(((Integer) value).doubleValue(), DoubleType.DOUBLE);
                }
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(Double.valueOf((String) value), DoubleType.DOUBLE);
                }
            }
            case REAL -> {
                if (value instanceof Double) {
                    return new VaradaPrimitiveConstant(((Double) value).floatValue(), RealType.REAL);
                }
                if (value instanceof Integer) {
                    return new VaradaPrimitiveConstant(((Integer) value).floatValue(), RealType.REAL);
                }
                if (value instanceof String) {
                    return new VaradaPrimitiveConstant(Float.valueOf((String) value), RealType.REAL);
                }
            }
        }
        throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Could not convert " + value + " to " + type);
    }

    private static TransformFunction convertApiTransformFunction(TransformFunctionData apiTransformFunction)
    {
        TransformFunction.TransformType transformType = convertApiTransformType(apiTransformFunction.transformType());
        TransformFunction transformFunction;

        if (apiTransformFunction.transformParams().isEmpty()) {
            transformFunction = new TransformFunction(transformType);
        }
        else {
            List<? extends VaradaConstant> transformParams = apiTransformFunction.transformParams().stream()
                    .filter(param -> (param instanceof VaradaPrimitiveConstantData))
                    .map(param -> convertApiVaradaPrimitiveConstant((VaradaPrimitiveConstantData) param))
                    .collect(Collectors.toList());
            transformFunction = new TransformFunction(transformType, transformParams);
        }
        return transformFunction;
    }

    public static WarmupColRuleData fromModel(WarmupRule warmupRule)
    {
        ImmutableSet<io.trino.plugin.varada.api.warmup.WarmupPredicateRule> predicates = warmupRule.getPredicates().stream().map(WarmupRuleApiMapper::convertFromModelPredicate).collect(toImmutableSet());
        io.trino.plugin.varada.api.warmup.WarmUpType warmUpType = convertModelWarmUpType(warmupRule.getWarmUpType());
        VaradaColumnData column = null;

        if (warmupRule.getVaradaColumn() instanceof TransformedColumn transformedColumn) {
            TransformFunctionData transformFunction = convertModelTransformFunction(transformedColumn.getTransformFunction());
            column = new TransformedColumnData(transformedColumn.getName(), transformFunction);
        }
        else if (warmupRule.getVaradaColumn() instanceof RegularColumn) {
            column = new RegularColumnData(warmupRule.getVaradaColumn().getName());
        }
        else if (warmupRule.getVaradaColumn() instanceof WildcardColumn) {
            column = new WildcardColumnData();
        }
        return new WarmupColRuleData(warmupRule.getId(), warmupRule.getSchema(), warmupRule.getTable(), column, warmUpType, warmupRule.getPriority(), Duration.ofSeconds(warmupRule.getTtl()), predicates);
    }

    private static io.trino.plugin.varada.api.warmup.WarmupPredicateRule convertFromModelPredicate(WarmupPredicateRule modelWarmupPredicateRule)
    {
        if (modelWarmupPredicateRule instanceof PartitionValueWarmupPredicateRule apiValuePredicate) {
            return new io.trino.plugin.varada.api.warmup.PartitionValueWarmupPredicateRule(apiValuePredicate.getColumnId(), apiValuePredicate.getValue());
        }
        else if (modelWarmupPredicateRule instanceof DateRangeSlidingWindowWarmupPredicateRule apiRangePredicate) {
            return new io.trino.plugin.varada.api.warmup.DateRangeSlidingWindowWarmupPredicateRule(apiRangePredicate.getColumnId(), apiRangePredicate.getStartRangeDaysBefore(), apiRangePredicate.getEndRangeDaysBefore(), apiRangePredicate.getWindowDateFormat());
        }
        else if (modelWarmupPredicateRule instanceof DateSlidingWindowWarmupPredicateRule apiSlidingPredicate) {
            return new io.trino.plugin.varada.api.warmup.DateSlidingWindowWarmupPredicateRule(apiSlidingPredicate.getColumnId(), apiSlidingPredicate.getWindowSizeDays(), apiSlidingPredicate.getWindowDateFormat(), apiSlidingPredicate.getBaseDate());
        }
        return null;
    }

    public static io.trino.plugin.varada.api.warmup.WarmUpType convertModelWarmUpType(WarmUpType modelWarmUpType)
    {
        return switch (modelWarmUpType) {
            case WARM_UP_TYPE_BASIC -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_BASIC;
            case WARM_UP_TYPE_DATA -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_DATA;
            case WARM_UP_TYPE_LUCENE -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_LUCENE;
            case WARM_UP_TYPE_BLOOM_HIGH -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_BLOOM_HIGH;
            case WARM_UP_TYPE_BLOOM_MEDIUM -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM;
            case WARM_UP_TYPE_BLOOM_LOW -> io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_BLOOM_LOW;
            default -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Unknown modelWarmUpType " + modelWarmUpType);
        };
    }

    private static TransformFunctionData.TransformType convertModelTransformType(TransformFunction.TransformType modelTransformType)
    {
        return switch (modelTransformType) {
            case LOWER -> TransformFunctionData.TransformType.LOWER;
            case UPPER -> TransformFunctionData.TransformType.UPPER;
            case DATE -> TransformFunctionData.TransformType.DATE;
            case ELEMENT_AT -> TransformFunctionData.TransformType.ELEMENT_AT;
            case JSON_EXTRACT_SCALAR -> TransformFunctionData.TransformType.JSON_EXTRACT_SCALAR;
            default -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Unknown modelTransformType " + modelTransformType);
        };
    }

    private static VaradaExpressionData.Type convertModelType(Type modelType)
    {
        if (modelType instanceof VarcharType) {
            return VaradaExpressionData.Type.VARCHAR;
        }
        if (modelType instanceof IntegerType) {
            return VaradaExpressionData.Type.INTEGER;
        }
        if (modelType instanceof BigintType) {
            return VaradaExpressionData.Type.BIGINT;
        }
        if (modelType instanceof SmallintType) {
            return VaradaExpressionData.Type.SMALLINT;
        }
        if (modelType instanceof DoubleType) {
            return VaradaExpressionData.Type.DOUBLE;
        }
        if (modelType instanceof RealType) {
            return VaradaExpressionData.Type.REAL;
        }
        throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Unknown modelType " + modelType);
    }

    private static VaradaPrimitiveConstantData convertModelVaradaPrimitiveConstant(VaradaPrimitiveConstant modelVaradaPrimitiveConstant)
    {
        return new VaradaPrimitiveConstantData(modelVaradaPrimitiveConstant.getValue(), convertModelType(modelVaradaPrimitiveConstant.getType()));
    }

    private static TransformFunctionData convertModelTransformFunction(TransformFunction modelTransformFunction)
    {
        TransformFunctionData.TransformType transformType = convertModelTransformType(modelTransformFunction.transformType());
        TransformFunctionData transformFunction;

        if (modelTransformFunction.transformParams().isEmpty()) {
            transformFunction = new TransformFunctionData(transformType);
        }
        else {
            ImmutableList<? extends VaradaConstantData> transformParams = modelTransformFunction.transformParams().stream()
                    .filter(param -> (param instanceof VaradaPrimitiveConstant))
                    .map(param -> convertModelVaradaPrimitiveConstant((VaradaPrimitiveConstant) param))
                    .collect(toImmutableList());
            transformFunction = new TransformFunctionData(transformType, transformParams);
        }
        return transformFunction;
    }
}
