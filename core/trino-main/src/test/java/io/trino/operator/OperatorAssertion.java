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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.util.StructuralTestUtil.appendToBlockBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public final class OperatorAssertion
{
    private static final Duration BLOCKED_DEFAULT_TIMEOUT = new Duration(10, MILLISECONDS);
    private static final Duration UNBLOCKED_DEFAULT_TIMEOUT = new Duration(1, SECONDS);

    private OperatorAssertion() {}

    public static List<Page> toPages(Operator operator, Iterator<Page> input)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartial(operator, input))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPages(Operator operator, Iterator<Page> input, boolean revokeMemoryWhenAddingPages)
    {
        return ImmutableList.<Page>builder()
                .addAll(toPagesPartial(operator, input, revokeMemoryWhenAddingPages))
                .addAll(finishOperator(operator))
                .build();
    }

    public static List<Page> toPagesPartial(Operator operator, Iterator<Page> input)
    {
        return toPagesPartial(operator, input, true);
    }

    public static List<Page> toPagesPartial(Operator operator, Iterator<Page> input, boolean revokeMemory)
    {
        // verify initial state
        assertThat(operator.isFinished()).isEqualTo(false);

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (int loopsSinceLastPage = 0; loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            if (revokeMemory) {
                handleMemoryRevoking(operator);
            }

            if (input.hasNext() && operator.needsInput()) {
                operator.addInput(input.next());
                loopsSinceLastPage = 0;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }
        }

        return outputPages.build();
    }

    public static List<Page> finishOperator(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        for (int loopsSinceLastPage = 0; !operator.isFinished() && loopsSinceLastPage < 1_000; loopsSinceLastPage++) {
            if (handledBlocked(operator)) {
                continue;
            }

            operator.finish();
            Page outputPage = operator.getOutput();
            if (outputPage != null && outputPage.getPositionCount() != 0) {
                outputPages.add(outputPage);
                loopsSinceLastPage = 0;
            }

            // revoke memory when output pages have started being produced
            handleMemoryRevoking(operator);
        }

        assertThat(operator.isFinished())
                .describedAs("Operator did not finish")
                .isEqualTo(true);
        assertThat(operator.needsInput())
                .describedAs("Operator still wants input")
                .isEqualTo(false);
        assertThat(operator.isBlocked().isDone())
                .describedAs("Operator is blocked")
                .isEqualTo(true);

        return outputPages.build();
    }

    private static boolean handledBlocked(Operator operator)
    {
        ListenableFuture<Void> isBlocked = operator.isBlocked();
        if (!isBlocked.isDone()) {
            tryGetFutureValue(isBlocked, 1, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private static void handleMemoryRevoking(Operator operator)
    {
        if (operator.getOperatorContext().getReservedRevocableBytes() > 0) {
            getFutureValue(operator.startMemoryRevoke());
            operator.finishMemoryRevoke();
        }
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input)
    {
        return toPages(operatorFactory, driverContext, input, true);
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages)
    {
        return toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, true);
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, boolean revokeMemoryWhenAddingPages, boolean closeOperatorFactory)
    {
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            if (closeOperatorFactory) {
                operatorFactory.noMoreOperators();
            }
            return toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static List<Page> toPages(OperatorFactory operatorFactory, DriverContext driverContext)
    {
        return toPages(operatorFactory, driverContext, ImmutableList.of());
    }

    public static MaterializedResult toMaterializedResult(Session session, List<Type> types, List<Page> pages)
    {
        // materialize pages
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(session, types);
        for (Page outputPage : pages) {
            resultBuilder.page(outputPage);
        }
        return resultBuilder.build();
    }

    public static SqlRow toRow(List<Type> parameterTypes, Object... values)
    {
        checkArgument(parameterTypes.size() == values.length, "parameterTypes.size(" + parameterTypes.size() + ") does not equal to values.length(" + values.length + ")");

        return buildRowValue(RowType.anonymous(parameterTypes), fields -> {
            for (int i = 0; i < values.length; i++) {
                appendToBlockBuilder(parameterTypes.get(i), values[i], fields.get(i));
            }
        });
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, List<Type> types, DriverContext driverContext, List<Page> input, List<Page> expected)
    {
        List<Page> actual = toPages(operatorFactory, driverContext, input);
        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(types, actual.get(i), expected.get(i));
        }
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, MaterializedResult expected)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, true);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages,
            boolean closeOperatorFactory)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, false, ImmutableList.of(), revokeMemoryWhenAddingPages, closeOperatorFactory);
    }

    public static void assertOperatorEquals(OperatorFactory operatorFactory, DriverContext driverContext, List<Page> input, MaterializedResult expected, boolean hashEnabled, List<Integer> hashChannels)
    {
        assertOperatorEquals(operatorFactory, driverContext, input, expected, hashEnabled, hashChannels, true);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEquals(
                operatorFactory,
                driverContext,
                input,
                expected,
                hashEnabled,
                hashChannels,
                revokeMemoryWhenAddingPages,
                true);
    }

    public static void assertOperatorEquals(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            List<Integer> hashChannels,
            boolean revokeMemoryWhenAddingPages,
            boolean closeOperatorFactory)
    {
        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages, closeOperatorFactory);
        if (hashEnabled && !hashChannels.isEmpty()) {
            // Drop the hashChannel for all pages
            pages = dropChannel(pages, hashChannels);
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, false);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean revokeMemoryWhenAddingPages)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, false, Optional.empty(), revokeMemoryWhenAddingPages);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel)
    {
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, hashEnabled, hashChannel, true);
    }

    public static void assertOperatorEqualsIgnoreOrder(
            OperatorFactory operatorFactory,
            DriverContext driverContext,
            List<Page> input,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel,
            boolean revokeMemoryWhenAddingPages)
    {
        assertPagesEqualIgnoreOrder(
                driverContext,
                toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages),
                expected,
                hashEnabled,
                hashChannel);
    }

    public static void assertPagesEqualIgnoreOrder(
            DriverContext driverContext,
            List<Page> actualPages,
            MaterializedResult expected,
            boolean hashEnabled,
            Optional<Integer> hashChannel)
    {
        if (hashEnabled && hashChannel.isPresent()) {
            // Drop the hashChannel for all pages
            actualPages = dropChannel(actualPages, ImmutableList.of(hashChannel.get()));
        }
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), actualPages);
        assertThat(actual.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
    }

    public static void assertOperatorIsBlocked(Operator operator)
    {
        assertOperatorIsBlocked(operator, BLOCKED_DEFAULT_TIMEOUT);
    }

    public static void assertOperatorIsBlocked(Operator operator, Duration timeout)
    {
        if (waitForOperatorToUnblock(operator, timeout)) {
            fail("Operator is expected to be blocked for at least " + timeout.toString());
        }
    }

    public static void assertOperatorIsUnblocked(Operator operator)
    {
        assertOperatorIsUnblocked(operator, UNBLOCKED_DEFAULT_TIMEOUT);
    }

    public static void assertOperatorIsUnblocked(Operator operator, Duration timeout)
    {
        if (!waitForOperatorToUnblock(operator, timeout)) {
            fail("Operator is expected to be unblocked within " + timeout.toString());
        }
    }

    private static boolean waitForOperatorToUnblock(Operator operator, Duration timeout)
    {
        try {
            operator.isBlocked().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        catch (TimeoutException expected) {
            return false;
        }
    }

    public static <T> List<T> without(List<T> list, Collection<Integer> indexes)
    {
        Set<Integer> indexesSet = ImmutableSet.copyOf(indexes);

        return IntStream.range(0, list.size())
                .filter(index -> !indexesSet.contains(index))
                .mapToObj(list::get)
                .collect(toImmutableList());
    }

    public static List<Page> dropChannel(List<Page> pages, List<Integer> channels)
    {
        List<Page> actualPages = new ArrayList<>();
        for (Page page : pages) {
            int channel = 0;
            Block[] blocks = new Block[page.getChannelCount() - channels.size()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                if (channels.contains(i)) {
                    continue;
                }
                blocks[channel++] = page.getBlock(i);
            }
            actualPages.add(new Page(blocks));
        }
        return actualPages;
    }
}
