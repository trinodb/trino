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
import io.trino.operator.index.PageRecordSet;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.CursorProcessorOutput;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.sql.relational.CallExpression;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCursorProcessorCompiler
{
    @Test
    void testHugeNumberOfProjections()
    {
        int projectionCount = 10_000;
        int positionCount = 512;
        long constantValue = 10;

        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        CallExpression addConstantExpression = call(
                functionResolution.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                field(0, BIGINT),
                constant(constantValue, BIGINT));

        CursorProcessorCompiler cursorProcessorCompiler = functionResolution.getCursorProcessorCompiler();
        Supplier<CursorProcessor> cursorProcessorSupplier = cursorProcessorCompiler.compileCursorProcessor(Optional.empty(), nCopies(projectionCount, addConstantExpression), "key");
        CursorProcessor cursorProcessor = cursorProcessorSupplier.get();

        List<Type> inputTypes = ImmutableList.of(BIGINT);
        List<Type> outputTypes = nCopies(projectionCount, BIGINT);

        Page inputPage = createSequencePage(inputTypes, positionCount);
        Block inputBlock = inputPage.getBlock(0);
        RecordCursor cursor = new PageRecordSet(inputTypes, inputPage).cursor();
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        PageBuilder outputPageBuilder = new PageBuilder(outputTypes);

        CursorProcessorOutput processorOutput;
        int processedPositions = 0;
        do {
            processorOutput = cursorProcessor.process(SESSION, yieldSignal, cursor, outputPageBuilder);

            Page outputPage = outputPageBuilder.build();
            outputPageBuilder.reset();

            assertThat(outputPage.getChannelCount()).isEqualTo(projectionCount);

            for (int channel = 0; channel < outputPage.getChannelCount(); channel++) {
                Block outputBlock = outputPage.getBlock(channel);

                for (int position = 0; position < outputBlock.getPositionCount(); position++) {
                    long input = BIGINT.getLong(inputBlock, processedPositions + position);
                    long output = BIGINT.getLong(outputBlock, position);
                    assertThat(output).isEqualTo(input + constantValue);
                }
            }

            processedPositions += outputPage.getPositionCount();
        } while (!processorOutput.isNoMoreRows());

        assertThat(processedPositions).isEqualTo(positionCount);
    }
}
