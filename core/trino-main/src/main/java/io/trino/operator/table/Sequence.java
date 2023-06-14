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
package io.trino.operator.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.operator.table.Sequence.SequenceFunctionSplit.MAX_SPLIT_SIZE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;

public class Sequence
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "sequence";

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new SequenceFunction(), getClass().getClassLoader());
    }

    public static class SequenceFunction
            extends AbstractConnectorTableFunction
    {
        private static final String START_ARGUMENT_NAME = "START";
        private static final String STOP_ARGUMENT_NAME = "STOP";
        private static final String STEP_ARGUMENT_NAME = "STEP";

        public SequenceFunction()
        {
            super(
                    BUILTIN_SCHEMA,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name(START_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .defaultValue(0L)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(STOP_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(STEP_ARGUMENT_NAME)
                                    .type(BIGINT)
                                    .defaultValue(1L)
                                    .build()),
                    new DescribedTable(descriptor(ImmutableList.of("sequential_number"), ImmutableList.of(BIGINT))));
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            Object startValue = ((ScalarArgument) arguments.get(START_ARGUMENT_NAME)).getValue();
            if (startValue == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Start is null");
            }

            Object stopValue = ((ScalarArgument) arguments.get(STOP_ARGUMENT_NAME)).getValue();
            if (stopValue == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Stop is null");
            }

            Object stepValue = ((ScalarArgument) arguments.get(STEP_ARGUMENT_NAME)).getValue();
            if (stepValue == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Step is null");
            }

            long start = (long) startValue;
            long stop = (long) stopValue;
            long step = (long) stepValue;

            if (start < stop && step <= 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Step must be positive for sequence [%s, %s]", start, stop));
            }

            if (start > stop && step >= 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Step must be negative for sequence [%s, %s]", start, stop));
            }

            return TableFunctionAnalysis.builder()
                    .handle(new SequenceFunctionHandle(start, stop, start == stop ? 0 : step))
                    .build();
        }
    }

    public record SequenceFunctionHandle(long start, long stop, long step)
            implements ConnectorTableFunctionHandle
    {}

    public static ConnectorSplitSource getSequenceFunctionSplitSource(SequenceFunctionHandle handle)
    {
        // using BigInteger to avoid long overflow since it's not in the main data processing loop
        BigInteger start = BigInteger.valueOf(handle.start());
        BigInteger stop = BigInteger.valueOf(handle.stop());
        BigInteger step = BigInteger.valueOf(handle.step());

        if (step.equals(BigInteger.ZERO)) {
            checkArgument(start.equals(stop), "start is not equal to stop for step = 0");
            return new FixedSplitSource(ImmutableList.of(new SequenceFunctionSplit(start.longValueExact(), stop.longValueExact())));
        }

        ImmutableList.Builder<SequenceFunctionSplit> splits = ImmutableList.builder();

        BigInteger totalSteps = stop.subtract(start).divide(step).add(BigInteger.ONE);
        BigInteger totalSplits = totalSteps.divide(BigInteger.valueOf(MAX_SPLIT_SIZE)).add(BigInteger.ONE);
        BigInteger[] stepsPerSplit = totalSteps.divideAndRemainder(totalSplits);
        BigInteger splitJump = stepsPerSplit[0].subtract(BigInteger.ONE).multiply(step);

        BigInteger splitStart = start;
        for (BigInteger i = BigInteger.ZERO; i.compareTo(totalSplits) < 0; i = i.add(BigInteger.ONE)) {
            BigInteger splitStop = splitStart.add(splitJump);
            // distribute the remaining steps between the initial splits, one step per split
            if (i.compareTo(stepsPerSplit[1]) < 0) {
                splitStop = splitStop.add(step);
            }
            splits.add(new SequenceFunctionSplit(splitStart.longValueExact(), splitStop.longValueExact()));
            splitStart = splitStop.add(step);
        }

        return new FixedSplitSource(splits.build());
    }

    public static class SequenceFunctionSplit
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(SequenceFunctionSplit.class);
        public static final int DEFAULT_SPLIT_SIZE = 1000000;
        public static final int MAX_SPLIT_SIZE = 1000000;

        // the first value of sub-sequence
        private final long start;

        // the last value of sub-sequence. this value is aligned so that it belongs to the sequence.
        private final long stop;

        @JsonCreator
        public SequenceFunctionSplit(@JsonProperty("start") long start, @JsonProperty("stop") long stop)
        {
            this.start = start;
            this.stop = stop;
        }

        @JsonProperty
        public long getStart()
        {
            return start;
        }

        @JsonProperty
        public long getStop()
        {
            return stop;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return ImmutableMap.builder()
                    .put("start", start)
                    .put("stop", stop)
                    .buildOrThrow();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }
    }

    public static TableFunctionProcessorProvider getSequenceFunctionProcessorProvider()
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
            {
                return new SequenceFunctionProcessor(((SequenceFunctionHandle) handle).step(), (SequenceFunctionSplit) split);
            }
        };
    }

    public static class SequenceFunctionProcessor
            implements TableFunctionSplitProcessor
    {
        private final PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        private final long step;
        private long start;
        private final long stop;
        private boolean finished;

        public SequenceFunctionProcessor(long step, SequenceFunctionSplit split)
        {
            this.step = step;
            this.start = split.getStart();
            this.stop = split.getStop();
        }

        @Override
        public TableFunctionProcessorState process()
        {
            checkState(pageBuilder.isEmpty(), "page builder not empty");

            if (finished) {
                return FINISHED;
            }

            BlockBuilder block = pageBuilder.getBlockBuilder(0);
            while (start != stop && !pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                BIGINT.writeLong(block, start);
                start += step;
            }
            if (!pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                BIGINT.writeLong(block, start);
                finished = true;
            }
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return produced(page);
        }
    }
}
