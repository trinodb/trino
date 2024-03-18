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

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_ARRAY_INT;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_BIGINT;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_BOOLEAN;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_CHAR;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_DATE;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_DECIMAL_LONG;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_DECIMAL_SHORT;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_DOUBLE;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_INTEGER;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_REAL;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_SMALLINT;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_TIME;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_TINYINT;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_VARCHAR;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BlockAppenderFactoryTest
{
    private BlockAppenderFactory blockAppenderFactory;
    private StubsStorageEngineConstants storageEngineConstants;

    @BeforeEach
    public void before()
    {
        storageEngineConstants = new StubsStorageEngineConstants();
        BlockTransformerFactory blockTransformerFactory = new BlockTransformerFactory();
        blockAppenderFactory = new BlockAppenderFactory(storageEngineConstants, mock(BufferAllocator.class), new GlobalConfiguration(), blockTransformerFactory);
    }

    static Stream<Arguments> params()
    {
        return Stream.of(
                Arguments.of(REC_TYPE_DATE, DateType.DATE, WARM_UP_TYPE_DATA, IntBlockAppender.class),
                Arguments.of(REC_TYPE_INTEGER, IntegerType.INTEGER, WARM_UP_TYPE_DATA, IntBlockAppender.class),
                Arguments.of(REC_TYPE_BOOLEAN, BooleanType.BOOLEAN, WARM_UP_TYPE_BASIC, BooleanBlockAppender.class),
                Arguments.of(REC_TYPE_TINYINT, TinyintType.TINYINT, WARM_UP_TYPE_BASIC, CrcTinyIntBlockAppender.class),
                Arguments.of(REC_TYPE_TINYINT, TinyintType.TINYINT, WARM_UP_TYPE_DATA, TinyIntBlockAppender.class),
                Arguments.of(REC_TYPE_SMALLINT, SmallintType.SMALLINT, WARM_UP_TYPE_BASIC, CrcSmallIntBlockAppender.class),
                Arguments.of(REC_TYPE_SMALLINT, SmallintType.SMALLINT, WARM_UP_TYPE_DATA, SmallIntBlockAppender.class),
                Arguments.of(REC_TYPE_BIGINT, BigintType.BIGINT, WARM_UP_TYPE_BASIC, CrcLongBlockAppender.class),
                Arguments.of(REC_TYPE_BIGINT, BigintType.BIGINT, WARM_UP_TYPE_DATA, LongBlockAppender.class),
                Arguments.of(REC_TYPE_REAL, RealType.REAL, WARM_UP_TYPE_BASIC, CrcRealBlockAppender.class),
                Arguments.of(REC_TYPE_REAL, RealType.REAL, WARM_UP_TYPE_DATA, RealBlockAppender.class),
                Arguments.of(REC_TYPE_DECIMAL_SHORT, DecimalType.createDecimalType(2), WARM_UP_TYPE_BASIC, CrcLongBlockAppender.class),
                Arguments.of(REC_TYPE_DECIMAL_SHORT, DecimalType.createDecimalType(2), WARM_UP_TYPE_DATA, LongBlockAppender.class),
                Arguments.of(REC_TYPE_DECIMAL_LONG, DecimalType.createDecimalType(20), WARM_UP_TYPE_BASIC, CrcLongDecimalBlockAppender.class),
                Arguments.of(REC_TYPE_DECIMAL_LONG, DecimalType.createDecimalType(20), WARM_UP_TYPE_DATA, LongDecimalBlockAppender.class),
                Arguments.of(REC_TYPE_DOUBLE, DoubleType.DOUBLE, WARM_UP_TYPE_DATA, DoubleBlockAppender.class),
                Arguments.of(REC_TYPE_DOUBLE, DoubleType.DOUBLE, WARM_UP_TYPE_BASIC, CrcDoubleBlockAppender.class),
                Arguments.of(REC_TYPE_TIME, TimeType.createTimeType(3), WARM_UP_TYPE_DATA, LongBlockAppender.class),
                Arguments.of(REC_TYPE_TIME, TimeType.createTimeType(3), WARM_UP_TYPE_BASIC, CrcLongBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.VARCHAR, WARM_UP_TYPE_BASIC, CrcStringBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.VARCHAR, WARM_UP_TYPE_DATA, VariableLengthStringBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.createVarcharType(5), WARM_UP_TYPE_BASIC, CrcStringBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.createVarcharType(5), WARM_UP_TYPE_DATA, VariableLengthStringBlockAppender.class),
                Arguments.of(REC_TYPE_CHAR, VarcharType.createVarcharType(5), WARM_UP_TYPE_BASIC, CrcStringBlockAppender.class),
                Arguments.of(REC_TYPE_CHAR, VarcharType.createVarcharType(5), WARM_UP_TYPE_DATA, FixedLengthStringBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.createVarcharType(5), WarmUpType.WARM_UP_TYPE_LUCENE, LuceneBlockAppender.class),
                Arguments.of(REC_TYPE_VARCHAR, VarcharType.VARCHAR, WarmUpType.WARM_UP_TYPE_LUCENE, LuceneBlockAppender.class),
                Arguments.of(REC_TYPE_CHAR, VarcharType.createVarcharType(5), WarmUpType.WARM_UP_TYPE_LUCENE, LuceneBlockAppender.class),
                Arguments.of(REC_TYPE_ARRAY_INT, new ArrayType(IntegerType.INTEGER), WARM_UP_TYPE_DATA, ArrayBlockAppender.class),
                Arguments.of(REC_TYPE_ARRAY_INT, new ArrayType(IntegerType.INTEGER), WARM_UP_TYPE_BASIC, CrcArrayBlockAppender.class));
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testInt(RecTypeCode recTypeCode, Type type, WarmUpType warmUpType, Class<BlockAppender> expectedResult)
    {
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getWarmUpType()).thenReturn(warmUpType);
        int recTypeLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
        when(warmUpElement.getRecTypeLength()).thenReturn(recTypeLength);
        WriteJuffersWarmUpElement juffersWarmUpElement = mock(WriteJuffersWarmUpElement.class);
        Optional<LuceneIndexer> luceneIndexerOpt = Optional.empty();
        if (warmUpType == WarmUpType.WARM_UP_TYPE_LUCENE) {
            luceneIndexerOpt = Optional.of(mock(LuceneIndexer.class));
        }
        BlockAppender blockAppender = blockAppenderFactory.createBlockAppender(warmUpElement, type, juffersWarmUpElement, luceneIndexerOpt);
        assertThat(blockAppender).isInstanceOf(expectedResult);
    }
}
