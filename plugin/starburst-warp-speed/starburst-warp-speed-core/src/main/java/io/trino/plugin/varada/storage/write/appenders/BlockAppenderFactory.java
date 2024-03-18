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
package io.trino.plugin.varada.storage.write.appenders;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.JsonExtract;
import io.trino.operator.scalar.JsonPath;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.type.cast.DateTimeUtils;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@Singleton
public class BlockAppenderFactory
{
    private static final Logger logger = Logger.get(BlockAppenderFactory.class);
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final int warmDataVarcharMaxLength;
    private final BlockTransformerFactory blockTransformerFactory;

    @Inject
    public BlockAppenderFactory(StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            GlobalConfiguration globalConfiguration,
            BlockTransformerFactory blockTransformerFactory)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.warmDataVarcharMaxLength = requireNonNull(globalConfiguration).getWarmDataVarcharMaxLength();
        this.blockTransformerFactory = requireNonNull(blockTransformerFactory);
    }

    public BlockAppender createBlockAppender(WarmUpElement warmUpElement,
            Type type,
            WriteJuffersWarmUpElement juffersWE,
            Optional<LuceneIndexer> luceneIndexerOpt)
    {
        BlockAppender blockAppender = switch (warmUpElement.getWarmUpType()) {
            case WARM_UP_TYPE_DATA -> createDataAppender(warmUpElement.getRecTypeCode(), juffersWE, warmUpElement, type);
            case WARM_UP_TYPE_BASIC, WARM_UP_TYPE_BLOOM_HIGH, WARM_UP_TYPE_BLOOM_MEDIUM, WARM_UP_TYPE_BLOOM_LOW -> createBasicAppender(warmUpElement,
                    type,
                    juffersWE);
            case WARM_UP_TYPE_LUCENE -> createLuceneAppender(warmUpElement.getRecTypeCode(), luceneIndexerOpt, juffersWE);
            default -> throw new RuntimeException("unknown warmup element type");
        };
        logger.debug("create appender of type=%s, warmUpType=%s, varadaColumn=%s", blockAppender.getClass().getName(), warmUpElement.getWarmUpType(), warmUpElement.getVaradaColumn());
        return blockAppender;
    }

    private BlockAppender createLuceneAppender(RecTypeCode recTypeCode,
            Optional<LuceneIndexer> luceneIndexerOpt,
            WriteJuffersWarmUpElement juffersWE)
    {
        if (luceneIndexerOpt.isEmpty()) {
            throw new WarmupException(
                    "Can't append a non-lucene column.",
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
        return switch (recTypeCode) {
            case REC_TYPE_BOOLEAN, REC_TYPE_TIMESTAMP, REC_TYPE_TIMESTAMP_WITH_TZ, REC_TYPE_TIME, REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT, REC_TYPE_INTEGER, REC_TYPE_DATE, REC_TYPE_REAL, REC_TYPE_SMALLINT, REC_TYPE_TINYINT, REC_TYPE_DOUBLE, REC_TYPE_DECIMAL_LONG, REC_TYPE_CHAR, REC_TYPE_VARCHAR ->
                    new LuceneBlockAppender(juffersWE, luceneIndexerOpt.get());
            case REC_TYPE_ARRAY_INT, REC_TYPE_ARRAY_BIGINT, REC_TYPE_ARRAY_VARCHAR, REC_TYPE_ARRAY_BOOLEAN, REC_TYPE_ARRAY_DOUBLE, REC_TYPE_ARRAY_CHAR ->
                    new LuceneArrayBlockAppender(juffersWE, luceneIndexerOpt.get());
            default -> throw new RuntimeException("unknown rec type code " + recTypeCode);
        };
    }

    private BlockAppender createBasicAppender(WarmUpElement warmUpElement,
            Type type,
            WriteJuffersWarmUpElement juffersWE)
    {
        BlockAppender res;

        if (warmUpElement.getVaradaColumn() instanceof TransformedColumn transformedColumn) {
            TransformFunction transformFunction = transformedColumn.getTransformFunction();

            if (Objects.equals(transformFunction, TransformFunction.LOWER) && warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_VARCHAR) {
                Function<Slice, Slice> transformedLowerFunction = (slice) -> Slices.utf8Slice(slice.toStringUtf8().toLowerCase(Locale.ROOT));
                res = new TransformedCrcStringBlockAppender(juffersWE, storageEngineConstants, bufferAllocator, type, false, transformedLowerFunction);
            }
            else if (Objects.equals(transformFunction, TransformFunction.UPPER) && warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_VARCHAR) {
                Function<Slice, Slice> transformedUpperFunction = (slice) -> Slices.utf8Slice(slice.toStringUtf8().toUpperCase(Locale.ROOT));
                res = new TransformedCrcStringBlockAppender(juffersWE, storageEngineConstants, bufferAllocator, type, false, transformedUpperFunction);
            }
            else if (Objects.equals(transformFunction, TransformFunction.DATE) && type == VarcharType.VARCHAR) {
                Function<BlockPosHolder, Integer> transformedDateFunction = (blockPosHolder) -> {
                    String val = blockPosHolder.getSlice().toStringUtf8();
                    return DateTimeUtils.parseDate(val);
                };
                res = new TransformedCrcDateBlockAppender(juffersWE, transformedDateFunction);
            }
            else if (transformFunction.transformType() == TransformFunction.TransformType.ELEMENT_AT) {
                BlockAppender basicAppender = createBasicAppender(warmUpElement.getRecTypeCode(), juffersWE, type);
                res = new CrcMapBlockAppender(juffersWE, basicAppender);
            }
            else if (transformFunction.transformType() == TransformFunction.TransformType.JSON_EXTRACT_SCALAR) {
                BiFunction<Slice, String, Slice> transformedJsonScalarFunction = (slice, pattern) -> {
                    JsonPath jsonPath = new JsonPath(pattern);
                    return JsonExtract.extract(slice, jsonPath.getScalarExtractor());
                };
                res = new TransformedCrcBiFunctionAppender(juffersWE,
                        storageEngineConstants,
                        bufferAllocator,
                        type,
                        false,
                        transformedJsonScalarFunction,
                        transformFunction.transformParams().get(0).getValueAsString());
            }
            else {
                throw new IllegalStateException("can't create block appender for WE: " + warmUpElement);
            }
        }
        else {
            res = createBasicAppender(warmUpElement.getRecTypeCode(), juffersWE, type);
        }
        return res;
    }

    private BlockAppender createBasicAppender(RecTypeCode recTypeCode, WriteJuffersWarmUpElement juffersWE, Type filterType)
    {
        return switch (recTypeCode) {
            case REC_TYPE_BOOLEAN -> new BooleanBlockAppender(juffersWE);
            case REC_TYPE_TIMESTAMP, REC_TYPE_TIMESTAMP_WITH_TZ, REC_TYPE_TIME, REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT -> new CrcLongBlockAppender(juffersWE);
            case REC_TYPE_INTEGER, REC_TYPE_DATE -> new CrcIntBlockAppender(juffersWE);
            case REC_TYPE_REAL -> new CrcRealBlockAppender(juffersWE);
            case REC_TYPE_SMALLINT -> new CrcSmallIntBlockAppender(juffersWE);
            case REC_TYPE_TINYINT -> new CrcTinyIntBlockAppender(juffersWE);
            case REC_TYPE_DOUBLE -> new CrcDoubleBlockAppender(juffersWE);
            case REC_TYPE_DECIMAL_LONG -> new CrcLongDecimalBlockAppender(juffersWE);
            case REC_TYPE_CHAR -> new CrcStringBlockAppender(juffersWE, storageEngineConstants, bufferAllocator, filterType, true);
            case REC_TYPE_VARCHAR -> new CrcStringBlockAppender(juffersWE, storageEngineConstants, bufferAllocator, filterType, false);
            case REC_TYPE_ARRAY_INT, REC_TYPE_ARRAY_BIGINT, REC_TYPE_ARRAY_VARCHAR, REC_TYPE_ARRAY_BOOLEAN, REC_TYPE_ARRAY_DOUBLE -> {
                CrcStringBlockAppender stringBlockAppender =
                        new CrcStringBlockAppender(juffersWE, storageEngineConstants, bufferAllocator, filterType, false);
                yield new CrcArrayBlockAppender(blockTransformerFactory, juffersWE, stringBlockAppender, filterType);
            }
            default -> throw new RuntimeException("unknown rec type code " + recTypeCode);
        };
    }

    private BlockAppender createDataAppender(RecTypeCode recTypeCode, WriteJuffersWarmUpElement juffersWE, WarmUpElement warmUpElement, Type type)
    {
        int weRecTypeLength = warmUpElement.getRecTypeLength();
        return switch (recTypeCode) {
            case REC_TYPE_BOOLEAN -> new BooleanBlockAppender(juffersWE);
            case REC_TYPE_TIMESTAMP, REC_TYPE_TIMESTAMP_WITH_TZ, REC_TYPE_TIME, REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT -> new LongBlockAppender(juffersWE);
            case REC_TYPE_INTEGER, REC_TYPE_DATE -> new IntBlockAppender(juffersWE);
            case REC_TYPE_REAL -> new RealBlockAppender(juffersWE);
            case REC_TYPE_SMALLINT -> new SmallIntBlockAppender(juffersWE);
            case REC_TYPE_TINYINT -> new TinyIntBlockAppender(juffersWE);
            case REC_TYPE_DOUBLE -> new DoubleBlockAppender(juffersWE);
            case REC_TYPE_DECIMAL_LONG -> new LongDecimalBlockAppender(juffersWE);
            case REC_TYPE_CHAR -> new FixedLengthStringBlockAppender(juffersWE, storageEngineConstants, weRecTypeLength, type);
            case REC_TYPE_VARCHAR -> new VariableLengthStringBlockAppender(juffersWE, storageEngineConstants, weRecTypeLength, type, warmDataVarcharMaxLength);
            case REC_TYPE_ARRAY_INT, REC_TYPE_ARRAY_BIGINT, REC_TYPE_ARRAY_VARCHAR, REC_TYPE_ARRAY_CHAR, REC_TYPE_ARRAY_BOOLEAN, REC_TYPE_ARRAY_TIMESTAMP, REC_TYPE_ARRAY_DOUBLE -> {
                VariableLengthStringBlockAppender varcharBlockAppender =
                        new VariableLengthStringBlockAppender(juffersWE, storageEngineConstants, weRecTypeLength, type, warmDataVarcharMaxLength);
                yield new ArrayBlockAppender(blockTransformerFactory, juffersWE, varcharBlockAppender, type);
            }
            default -> throw new RuntimeException("unknown rec type code " + recTypeCode);
        };
    }
}
