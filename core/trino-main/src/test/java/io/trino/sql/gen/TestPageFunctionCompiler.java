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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.relational.CallExpression;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFunctionCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final CallExpression ADD_10_EXPRESSION = call(
            FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
            field(0, BIGINT),
            constant(10L, BIGINT));

    @Test
    public void testFailureDoesNotCorruptFutureResults()
    {
        PageFunctionCompiler functionCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty());
        PageProjection projection = projectionSupplier.get();

        // process good page and verify we got the expected number of result rows
        Page goodPage = createLongBlockPage(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Block goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());

        // addition will throw due to integer overflow
        Page badPage = createLongBlockPage(0, 1, 2, 3, 4, Long.MAX_VALUE);
        assertTrinoExceptionThrownBy(() -> project(projection, badPage, SelectedPositions.positionsRange(0, 100)))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // running the good page should still work
        // if block builder in generated code was not reset properly, we could get junk results after the failure
        goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());
    }

    @Test
    public void testGeneratedClassName()
    {
        PageFunctionCompiler functionCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        String planNodeId = "7";
        String stageId = "20170707_223500_67496_zguwn.2";
        String classSuffix = stageId + "_" + planNodeId;
        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of(classSuffix));
        PageProjection projection = projectionSupplier.get();
        Work<Block> work = projection.project(SESSION, new DriverYieldSignal(), createLongBlockPage(0), SelectedPositions.positionsRange(0, 1));
        // class name should look like PageProjectionOutput_20170707_223500_67496_zguwn_2_7_XX
        assertThat(work.getClass().getSimpleName().startsWith("PageProjectionWork_" + stageId.replace('.', '_') + "_" + planNodeId)).isTrue();
    }

    @Test
    public void testCache()
    {
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        assertThat(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty())).isSameAs(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty()));
        assertThat(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint"))).isSameAs(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint")));
        assertThat(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint"))).isSameAs(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint2")));
        assertThat(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty())).isSameAs(cacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint2")));

        PageFunctionCompiler noCacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();
        assertThat(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty()))
                .isNotSameAs(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty()));
        assertThat(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint")))
                .isNotSameAs(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint")));
        assertThat(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint")))
                .isNotSameAs(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint2")));
        assertThat(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.empty()))
                .isNotSameAs(noCacheCompiler.compileProjection(ADD_10_EXPRESSION, Optional.of("hint2")));
    }

    private Block project(PageProjection projection, Page page, SelectedPositions selectedPositions)
    {
        Work<Block> work = projection.project(SESSION, new DriverYieldSignal(), page, selectedPositions);
        assertThat(work.process()).isTrue();
        return work.getResult();
    }

    private static Page createLongBlockPage(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return new Page(builder.build());
    }
}
