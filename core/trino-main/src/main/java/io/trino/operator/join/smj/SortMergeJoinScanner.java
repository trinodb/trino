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
package io.trino.operator.join.smj;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.type.BlockTypeOperators;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class SortMergeJoinScanner
        implements Runnable
{
    private final String taskId;
    private final PageIterator streamedIter;
    private final PageIterator bufferedIter;

    protected final List<Type> probeTypes;
    protected final List<Integer> probeEquiJoinClauseChannels;
    protected final List<Integer> probeOutputChannels;
    protected final List<Type> buildTypes;
    protected final List<Integer> buildEquiJoinClauseChannels;
    protected final List<Integer> buildOutputChannels;
    protected PageBuilder joinPageBuilder;

    protected Page curProbePage;
    protected int curProbePosition;
    protected Row streamedRow;
    protected Page curBuildPage;
    protected int curBuildPosition;
    protected Row bufferedRow;

    protected SpillableMatchedPages bufferedMatches;

    private List<BlockTypeOperators.BlockPositionComparison> equiJoinPositionComparison;

    private boolean finish;

    private Integer maxBufferPageSize;
    private LinkedBlockingQueue<Page> joinPages;

    private boolean started;

    private Object innerLock = new Object();
    private Object externLock = new Object();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public SortMergeJoinScanner(String taskId, int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier, PageIterator streamedIter, PageIterator bufferedIter, BlockTypeOperators blockTypeOperators,
            List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels,
            List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        this.taskId = taskId;
        this.streamedIter = streamedIter;
        this.bufferedIter = bufferedIter;
        this.probeTypes = probeTypes;
        this.probeEquiJoinClauseChannels = probeEquiJoinClauseChannels;
        this.probeOutputChannels = probeOutputChannels;
        this.buildTypes = buildTypes;
        this.buildEquiJoinClauseChannels = buildEquiJoinClauseChannels;
        this.buildOutputChannels = buildOutputChannels;
        this.joinPageBuilder = createJoinPageBuilder();
        this.equiJoinPositionComparison = probeEquiJoinClauseChannels.stream()
                .map(probeTypes::get).map(blockTypeOperators::getComparisonUnorderedFirstOperator).collect(toImmutableList());

        this.curBuildPosition = -1;
        this.finish = false;
        this.bufferedMatches = new SpillableMatchedPages(numRowsInMemoryBufferThreshold, spillerSupplier);
        this.streamedRow = new Row();
        this.streamedRow.setEquiJoinClauseChannels(probeEquiJoinClauseChannels);
        this.bufferedRow = new Row();
        this.bufferedRow.setEquiJoinClauseChannels(buildEquiJoinClauseChannels);

        this.maxBufferPageSize = 5;
        this.joinPages = new LinkedBlockingQueue<>(this.maxBufferPageSize);
        this.started = false;
    }

    void innerWait(Predicate predicate)
    {
        while (true) {
            synchronized (externLock) {
                externLock.notify();
            }
            synchronized (innerLock) {
                try {
                    innerLock.wait();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (predicate == null || predicate.test(null)) {
                    break;
                }
            }
        }
    }

    void externWait()
    {
        synchronized (innerLock) {
            innerLock.notify();
        }
        synchronized (externLock) {
            try {
                externLock.wait(100);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract void doJoin()
            throws Exception;

    @Override
    public void run()
    {
        try {
            doJoin();

            Page page = joinPageBuilder.build();
            joinPageBuilder.reset();
            if (page.getPositionCount() > 0) {
                joinPages.put(page);
            }
            bufferedIter.close();
            streamedIter.close();
        }
        catch (Throwable e) {
            failure.compareAndSet(null, e);
        }

        finish = true;
    }

    public Page process()
    {
        throwIfFailed();
        if (!started) {
            new Thread(this, "smj-" + this.taskId).start();
            started = true;
        }

        if (!finish) {
            externWait();
        }
        return joinPages.poll();
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    protected PageBuilder createJoinPageBuilder()
    {
        return new PageBuilder(ImmutableList.<Type>builder()
                .addAll(probeOutputChannels.stream()
                        .map(probeTypes::get)
                        .collect(toImmutableList()))
                .addAll(buildOutputChannels.stream()
                        .map(buildTypes::get)
                        .collect(toImmutableList()))
                .build());
    }

    protected void consumeMatchingRows()
            throws Exception
    {
        Iterator<Row> iterator = bufferedMatches.getOrCreateIterator();
        while (iterator.hasNext()) {
            Row build = iterator.next();
            appendRow(streamedRow.getPage(), streamedRow.getPosition(), build.getPage(), build.getPosition());
        }

        bufferedMatches.resetIterator();
        streamedRow.reset();
    }

    protected void bufferMatchingRows()
    {
        bufferedMatches.clear();
        do {
            bufferedMatches.insertRow(bufferedRow);
            advancedBuffered();
        }
        while (bufferedRow.isExist() && compare(streamedRow.getPage(), streamedRow.getPosition(), bufferedRow.getPage(), bufferedRow.getPosition()) == 0);
        bufferedMatches.finishInsert();
    }

    protected void appendRow(Page probePage, int probePosition,
            Page buildPage, int buildPosition)
            throws Exception
    {
        Page page = appendRowToPageBuilder(probePage, probePosition, probeTypes, probeOutputChannels,
                buildPage, buildPosition, buildTypes, buildOutputChannels);
        if (page != null) {
            if (joinPages.size() >= maxBufferPageSize) {
                innerWait(o -> joinPages.size() < maxBufferPageSize);
            }
            joinPages.put(page);
        }
    }

    protected Page appendRowToPageBuilder(Page probePage, int probePosition, List<Type> probeTypes, List<Integer> probeOutputChannels,
            Page buildPage, int buildPosition, List<Type> buildTypes, List<Integer> buildOutputChannels)
    {
        joinPageBuilder.declarePosition();
        int i = 0;
        for (Integer channel : probeOutputChannels) {
            if (probePage != null) {
                probeTypes.get(channel).appendTo(probePage.getBlock(channel), probePosition, joinPageBuilder.getBlockBuilder(i));
            }
            else {
                joinPageBuilder.getBlockBuilder(i).appendNull();
            }
            i++;
        }

        for (Integer channel : buildOutputChannels) {
            if (buildPage != null) {
                buildTypes.get(channel).appendTo(buildPage.getBlock(channel), buildPosition, joinPageBuilder.getBlockBuilder(i));
            }
            else {
                joinPageBuilder.getBlockBuilder(i).appendNull();
            }
            i++;
        }

        Page result = null;
        if (joinPageBuilder.isFull()) {
            result = joinPageBuilder.build();
            joinPageBuilder.reset();
        }
        return result;
    }

    protected long compare(Page probePage, int probePosition,
            Page buildPage, int buildPosition)
    {
        long compare = 0;
        for (int i = 0; i < probeEquiJoinClauseChannels.size(); i++) {
            Block probe = probePage.getBlock(probeEquiJoinClauseChannels.get(i));
            Block build = buildPage.getBlock(buildEquiJoinClauseChannels.get(i));
            if (probe.isNull(probePosition) && build.isNull(buildPosition)) {
                continue;
            }
            else if (probe.isNull(probePosition) && !build.isNull(buildPosition)) {
                return 1;
            }
            else if (!probe.isNull(probePosition) && build.isNull(buildPosition)) {
                return -1;
            }

            compare = equiJoinPositionComparison.get(i).compare(probe, probePosition, build, buildPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return compare;
    }

    protected boolean advancedStreamed()
    {
        if (curProbePage == null || curProbePage.getPositionCount() - 1 == curProbePosition) {
            if (streamedIter.hasNext()) {
                curProbePage = streamedIter.next();
                curProbePosition = 0;
                streamedRow.set(curProbePage, curProbePosition);
                return true;
            }
            else {
                innerWait(o -> streamedIter.hasNext() || streamedIter.isFinished());
                if (streamedIter.hasNext()) {
                    return advancedStreamed();
                }
            }
        }
        else {
            curProbePosition++;
            streamedRow.set(curProbePage, curProbePosition);
            return true;
        }
        curProbePage = null;
        curProbePosition = -1;
        streamedRow.reset();
        return false;
    }

    protected boolean advancedBuffered()
    {
        if (curBuildPage == null || curBuildPage.getPositionCount() - 1 == curBuildPosition) {
            if (bufferedIter.hasNext()) {
                curBuildPage = bufferedIter.next();
                curBuildPosition = 0;
                bufferedRow.set(curBuildPage, curBuildPosition);
                return true;
            }
            else {
                innerWait(o -> bufferedIter.hasNext() || bufferedIter.isFinished());
                if (bufferedIter.hasNext()) {
                    return advancedBuffered();
                }
            }
        }
        else {
            curBuildPosition++;
            bufferedRow.set(curBuildPage, curBuildPosition);
            return true;
        }

        curBuildPage = null;
        curBuildPosition = -1;
        bufferedRow.reset();
        return false;
    }

    protected boolean advancedBufferedToRowWithNullFreeJoinKey()
    {
        while (advancedBuffered()) {
            if (!isRowKeyAnyNull(bufferedRow)) {
                break;
            }
        }
        return bufferedRow.isExist();
    }

    protected boolean isRowKeyAnyNull(Row row)
    {
        for (int i = 0; i < row.getEquiJoinClauseChannels().size(); i++) {
            if (row.getPage().getBlock(row.getEquiJoinClauseChannels().get(i)).isNull(row.getPosition())) {
                return true;
            }
        }
        return false;
    }

    public boolean isFinished()
    {
        return finish && joinPages.isEmpty();
    }

    public SpillableMatchedPages getBufferedMatches()
    {
        return bufferedMatches;
    }
}
