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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

//
// NOTE:  As a general strategy the methods should "stage" a change and only
// process the actual change before lock release (DriverLockResult.close()).
// The assures that only one thread will be working with the operators at a
// time and state changer threads are not blocked.
//
public class Driver
        implements Closeable
{
    private static final Logger log = Logger.get(Driver.class);

    private static final Duration UNLIMITED_DURATION = new Duration(Long.MAX_VALUE, NANOSECONDS);

    private final DriverContext driverContext;
    private final List<Operator> activeOperators;
    // this is present only for debugging
    @SuppressWarnings("unused")
    private final List<Operator> allOperators;
    private final Optional<SourceOperator> sourceOperator;

    // This variable acts as a staging area. When new splits (encapsulated in SplitAssignment) are
    // provided to a Driver, the Driver will not process them right away. Instead, the splits are
    // added to this staging area. This staging area will be drained asynchronously. That's when
    // the new splits get processed.
    private final AtomicReference<SplitAssignment> pendingSplitAssignmentUpdates = new AtomicReference<>();
    private final Map<Operator, ListenableFuture<Void>> revokingOperators = new HashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);
    private final SettableFuture<Void> destroyedFuture = SettableFuture.create();

    private final DriverLock exclusiveLock = new DriverLock(state, destroyedFuture);

    @GuardedBy("exclusiveLock")
    private SplitAssignment currentSplitAssignment;

    private final AtomicReference<SettableFuture<Void>> driverBlockedFuture = new AtomicReference<>();

    private enum State
    {
        ALIVE, NEED_DESTRUCTION, DESTROYING, DESTROYED
    }

    public static Driver createDriver(DriverContext driverContext, List<Operator> operators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(operators, "operators is null");
        Driver driver = new Driver(driverContext, operators);
        driver.initialize();
        return driver;
    }

    @VisibleForTesting
    public static Driver createDriver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(firstOperator, "firstOperator is null");
        requireNonNull(otherOperators, "otherOperators is null");
        ImmutableList<Operator> operators = ImmutableList.<Operator>builder()
                .add(firstOperator)
                .add(otherOperators)
                .build();
        return createDriver(driverContext, operators);
    }

    private Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.allOperators = ImmutableList.copyOf(requireNonNull(operators, "operators is null"));
        checkArgument(allOperators.size() > 1, "At least two operators are required");
        this.activeOperators = new ArrayList<>(operators);
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        Optional<SourceOperator> sourceOperator = Optional.empty();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                checkArgument(sourceOperator.isEmpty(), "There must be at most one SourceOperator");
                sourceOperator = Optional.of((SourceOperator) operator);
            }
        }
        this.sourceOperator = sourceOperator;

        currentSplitAssignment = sourceOperator.map(operator -> new SplitAssignment(operator.getSourceId(), ImmutableSet.of(), false)).orElse(null);
        // initially the driverBlockedFuture is not blocked (it is completed)
        SettableFuture<Void> future = SettableFuture.create();
        future.set(null);
        driverBlockedFuture.set(future);
    }

    // the memory revocation request listeners are added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        activeOperators.stream()
                .map(Operator::getOperatorContext)
                .forEach(operatorContext -> operatorContext.setMemoryRevocationRequestListener(() -> driverBlockedFuture.get().set(null)));
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public ListenableFuture<Void> getDestroyedFuture()
    {
        return destroyedFuture;
    }

    @Override
    public void close()
    {
        // mark the service for destruction
        if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
            return;
        }

        // set the yield signal and interrupt any actively running driver to stop them as soon as possible
        driverContext.getYieldSignal().yieldImmediatelyForTermination();
        exclusiveLock.interruptCurrentOwner();

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        tryWithLockUninterruptibly(() -> TRUE);
    }

    public boolean isFinished()
    {
        checkLockNotHeld("Cannot check finished status while holding the driver lock");

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        Optional<Boolean> result = tryWithLockUninterruptibly(this::isTerminatingOrDoneInternal);
        return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isTerminatingOrDone());
    }

    @GuardedBy("exclusiveLock")
    private boolean isTerminatingOrDoneInternal()
    {
        checkLockHeld("Lock must be held to call isTerminatingOrDoneInternal");

        boolean terminatingOrDone = state.get() != State.ALIVE || activeOperators.isEmpty() || activeOperators.get(activeOperators.size() - 1).isFinished() || driverContext.isTerminatingOrDone();
        if (terminatingOrDone) {
            state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
        }
        return terminatingOrDone;
    }

    public void updateSplitAssignment(SplitAssignment splitAssignment)
    {
        checkLockNotHeld("Cannot update assignments while holding the driver lock");
        checkArgument(
                sourceOperator.isPresent() && sourceOperator.get().getSourceId().equals(splitAssignment.getPlanNodeId()),
                "splitAssignment is for a plan node that is different from this Driver's source node");

        // stage the new updates
        pendingSplitAssignmentUpdates.updateAndGet(current -> current == null ? splitAssignment : current.update(splitAssignment));

        // attempt to get the lock and process the updates we staged above
        // updates will be processed in close if and only if we got the lock
        tryWithLockUninterruptibly(() -> TRUE);
    }

    @GuardedBy("exclusiveLock")
    private void processNewSources()
    {
        checkLockHeld("Lock must be held to call processNewSources");

        // only update if the driver is still alive
        if (state.get() != State.ALIVE) {
            return;
        }

        SplitAssignment splitAssignment = pendingSplitAssignmentUpdates.getAndSet(null);
        if (splitAssignment == null) {
            return;
        }

        // merge the current assignment and the specified assignment
        SplitAssignment newAssignment = currentSplitAssignment.update(splitAssignment);

        // if the update contains no new data, just return
        if (newAssignment == currentSplitAssignment) {
            return;
        }

        // determine new splits to add
        Set<ScheduledSplit> newSplits = Sets.difference(newAssignment.getSplits(), currentSplitAssignment.getSplits());

        // add new splits
        SourceOperator sourceOperator = this.sourceOperator.orElseThrow(VerifyException::new);
        for (ScheduledSplit newSplit : newSplits) {
            Split split = newSplit.getSplit();

            sourceOperator.addSplit(split);
        }

        // set no more splits
        if (newAssignment.isNoMoreSplits()) {
            sourceOperator.noMoreSplits();
        }

        currentSplitAssignment = newAssignment;
    }

    public ListenableFuture<Void> processForDuration(Duration duration)
    {
        return process(duration, Integer.MAX_VALUE);
    }

    public ListenableFuture<Void> processForNumberOfIterations(int maxIterations)
    {
        return process(UNLIMITED_DURATION, maxIterations);
    }

    public ListenableFuture<Void> processUntilBlocked()
    {
        return process(UNLIMITED_DURATION, Integer.MAX_VALUE);
    }

    @VisibleForTesting
    public ListenableFuture<Void> process(Duration maxRuntime, int maxIterations)
    {
        checkLockNotHeld("Cannot process for a duration while holding the driver lock");

        requireNonNull(maxRuntime, "maxRuntime is null");
        checkArgument(maxIterations > 0, "maxIterations must be greater than zero");

        // if the driver is blocked we don't need to continue
        SettableFuture<Void> blockedFuture = driverBlockedFuture.get();
        if (!blockedFuture.isDone()) {
            return blockedFuture;
        }

        long maxRuntimeInNanos = maxRuntime.roundTo(TimeUnit.NANOSECONDS);

        Optional<ListenableFuture<Void>> result = tryWithLock(100, TimeUnit.MILLISECONDS, true, () -> {
            OperationTimer operationTimer = createTimer();
            driverContext.startProcessTimer();
            driverContext.getYieldSignal().setWithDelay(maxRuntimeInNanos, driverContext.getYieldExecutor());
            try {
                long start = System.nanoTime();
                int iterations = 0;
                while (!isTerminatingOrDoneInternal()) {
                    ListenableFuture<Void> future = processInternal(operationTimer);
                    iterations++;
                    if (!future.isDone()) {
                        return updateDriverBlockedFuture(future);
                    }
                    if (System.nanoTime() - start >= maxRuntimeInNanos || iterations >= maxIterations) {
                        break;
                    }
                }
            }
            catch (Throwable t) {
                List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
                if (interrupterStack == null) {
                    driverContext.failed(t);
                    throw t;
                }

                // Driver thread was interrupted which should only happen if the task is already finished.
                // If this becomes the actual cause of a failed query there is a bug in the task state machine.
                Exception exception = new Exception("Interrupted By");
                exception.setStackTrace(interrupterStack.toArray(StackTraceElement[]::new));
                TrinoException newException = new TrinoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted", exception);
                newException.addSuppressed(t);
                driverContext.failed(newException);
                throw newException;
            }
            finally {
                driverContext.getYieldSignal().reset();
                driverContext.recordProcessed(operationTimer);
            }
            return NOT_BLOCKED;
        });
        return result.orElse(NOT_BLOCKED);
    }

    private OperationTimer createTimer()
    {
        return new OperationTimer(
                driverContext.isCpuTimerEnabled(),
                driverContext.isCpuTimerEnabled() && driverContext.isPerOperatorCpuTimerEnabled());
    }

    private ListenableFuture<Void> updateDriverBlockedFuture(ListenableFuture<Void> sourceBlockedFuture)
    {
        // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
        // or any of the operators gets a memory revocation request
        SettableFuture<Void> newDriverBlockedFuture = SettableFuture.create();
        driverBlockedFuture.set(newDriverBlockedFuture);
        sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

        // it's possible that memory revoking is requested for some operator
        // before we update driverBlockedFuture above and we don't want to miss that
        // notification, so we check to see whether that's the case before returning.
        boolean memoryRevokingRequested = activeOperators.stream()
                .filter(operator -> !revokingOperators.containsKey(operator))
                .map(Operator::getOperatorContext)
                .anyMatch(OperatorContext::isMemoryRevokingRequested);

        if (memoryRevokingRequested) {
            newDriverBlockedFuture.set(null);
        }

        return newDriverBlockedFuture;
    }

    @GuardedBy("exclusiveLock")
    private ListenableFuture<Void> processInternal(OperationTimer operationTimer)
    {
        checkLockHeld("Lock must be held to call processInternal");

        handleMemoryRevoke();

        processNewSources();

        // One of the operators is already finished (and removed from activeOperators list). Finish bottommost operator.
        // Some operators (LookupJoinOperator and HashBuildOperator) are broken and requires finish to be called continuously
        // TODO remove the second part of the if statement, when these operators are fixed
        // Note: finish should not be called on the natural source of the pipeline as this could cause the task to finish early
        if (!activeOperators.isEmpty() && activeOperators.size() != allOperators.size()) {
            Operator rootOperator = activeOperators.get(0);
            rootOperator.finish();
            rootOperator.getOperatorContext().recordFinish(operationTimer);
        }

        boolean movedPage = false;
        for (int i = 0; i < activeOperators.size() - 1 && !driverContext.isTerminatingOrDone(); i++) {
            Operator current = activeOperators.get(i);
            Operator next = activeOperators.get(i + 1);

            // skip blocked operator
            if (getBlockedFuture(current).isPresent()) {
                continue;
            }

            // if the current operator is not finished and next operator isn't blocked and needs input...
            if (!current.isFinished() && getBlockedFuture(next).isEmpty() && next.needsInput()) {
                // get an output page from current operator
                Page page = current.getOutput();
                current.getOperatorContext().recordGetOutput(operationTimer, page);

                // if we got an output page, add it to the next operator
                if (page != null && page.getPositionCount() != 0) {
                    next.addInput(page);
                    next.getOperatorContext().recordAddInput(operationTimer, page);
                    movedPage = true;
                }

                if (current instanceof SourceOperator) {
                    movedPage = true;
                }
            }

            // if current operator is finished...
            if (current.isFinished()) {
                // let next operator know there will be no more data
                next.finish();
                next.getOperatorContext().recordFinish(operationTimer);
            }
        }

        for (int index = activeOperators.size() - 1; index >= 0; index--) {
            if (activeOperators.get(index).isFinished()) {
                // close and remove this operator and all source operators
                List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                Throwable throwable = closeAndDestroyOperators(finishedOperators);
                finishedOperators.clear();
                if (throwable != null) {
                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }
                // Finish the next operator, which is now the first operator.
                if (!activeOperators.isEmpty()) {
                    Operator newRootOperator = activeOperators.get(0);
                    newRootOperator.finish();
                    newRootOperator.getOperatorContext().recordFinish(operationTimer);
                }
                break;
            }
        }

        // if we did not move any pages, check if we are blocked
        if (!movedPage) {
            List<Operator> blockedOperators = new ArrayList<>();
            List<ListenableFuture<Void>> blockedFutures = new ArrayList<>();
            for (Operator operator : activeOperators) {
                Optional<ListenableFuture<Void>> blocked = getBlockedFuture(operator);
                if (blocked.isPresent()) {
                    blockedOperators.add(operator);
                    blockedFutures.add(blocked.get());
                }
            }

            if (!blockedFutures.isEmpty()) {
                // allow for operators to unblock drivers when they become finished
                for (Operator operator : activeOperators) {
                    operator.getOperatorContext().getFinishedFuture().ifPresent(blockedFutures::add);
                }

                // unblock when the first future is complete
                ListenableFuture<Void> blocked = firstFinishedFuture(blockedFutures);
                // driver records serial blocked time
                driverContext.recordBlocked(blocked);
                // each blocked operator is responsible for blocking the execution
                // until one of the operators can continue
                for (Operator operator : blockedOperators) {
                    operator.getOperatorContext().recordBlocked(blocked);
                }
                return blocked;
            }
        }

        return NOT_BLOCKED;
    }

    @GuardedBy("exclusiveLock")
    private void handleMemoryRevoke()
    {
        for (int i = 0; i < activeOperators.size() && !driverContext.isTerminatingOrDone(); i++) {
            Operator operator = activeOperators.get(i);

            if (revokingOperators.containsKey(operator)) {
                checkOperatorFinishedRevoking(operator);
            }
            else if (operator.getOperatorContext().isMemoryRevokingRequested()) {
                ListenableFuture<Void> future = operator.startMemoryRevoke();
                revokingOperators.put(operator, future);
                checkOperatorFinishedRevoking(operator);
            }
        }
    }

    @GuardedBy("exclusiveLock")
    private void checkOperatorFinishedRevoking(Operator operator)
    {
        ListenableFuture<Void> future = revokingOperators.get(operator);
        if (future.isDone()) {
            getFutureValue(future); // propagate exception if there was some
            revokingOperators.remove(operator);
            operator.finishMemoryRevoke();
            operator.getOperatorContext().resetMemoryRevokingRequested();
        }
    }

    @GuardedBy("exclusiveLock")
    private void destroyIfNecessary()
    {
        checkLockHeld("Lock must be held to call destroyIfNecessary");

        if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYING)) {
            return;
        }

        // if we get an error while closing a driver, record it and we will throw it at the end
        Throwable inFlightException = null;
        try {
            inFlightException = closeAndDestroyOperators(activeOperators);
            if (driverContext.getMemoryUsage() > 0) {
                log.error("Driver still has memory reserved after freeing all operator memory.");
            }
            if (driverContext.getRevocableMemoryUsage() > 0) {
                log.error("Driver still has revocable memory reserved after freeing all operator memory. Freeing it.");
            }
            driverContext.finished();
        }
        catch (Throwable t) {
            // this shouldn't happen but be safe
            inFlightException = addSuppressedException(
                    inFlightException,
                    t,
                    "Error destroying driver for task %s",
                    driverContext.getTaskId());
        }
        finally {
            // Mark destruction as having completed after driverContext.finished() is complete
            state.set(State.DESTROYED);
        }

        if (inFlightException != null) {
            // this will always be an Error or Runtime
            throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
        }
    }

    private Throwable closeAndDestroyOperators(List<Operator> operators)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        Throwable inFlightException = null;
        try {
            for (Operator operator : operators) {
                try {
                    operator.close();
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error closing operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
                try {
                    operator.getOperatorContext().destroy();
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error freeing all allocated memory for operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return inFlightException;
    }

    private Optional<ListenableFuture<Void>> getBlockedFuture(Operator operator)
    {
        ListenableFuture<Void> blocked = revokingOperators.get(operator);
        if (blocked != null) {
            // We mark operator as blocked regardless of blocked.isDone(), because finishMemoryRevoke has not been called yet.
            return Optional.of(blocked);
        }
        blocked = operator.isBlocked();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForRevocableMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        return Optional.empty();
    }

    @FormatMethod
    private static Throwable addSuppressedException(Throwable inFlightException, Throwable newException, String message, final Object... args)
    {
        if (newException instanceof Error) {
            if (inFlightException == null) {
                inFlightException = newException;
            }
            else {
                // Self-suppression not permitted
                if (inFlightException != newException) {
                    inFlightException.addSuppressed(newException);
                }
            }
        }
        else {
            // log normal exceptions instead of rethrowing them
            log.error(newException, message, args);
        }
        return inFlightException;
    }

    private synchronized void checkLockNotHeld(String message)
    {
        checkState(!exclusiveLock.isHeldByCurrentThread(), message);
    }

    @GuardedBy("exclusiveLock")
    private synchronized void checkLockHeld(String message)
    {
        checkState(exclusiveLock.isHeldByCurrentThread(), message);
    }

    private static ListenableFuture<Void> firstFinishedFuture(List<ListenableFuture<Void>> futures)
    {
        if (futures.size() == 1) {
            return futures.get(0);
        }

        SettableFuture<Void> result = SettableFuture.create();

        for (ListenableFuture<Void> future : futures) {
            future.addListener(() -> result.set(null), directExecutor());
        }

        return result;
    }

    /**
     * Try to acquire the {@code exclusiveLock} immediately and run a {@code task}
     * The task will not be interrupted if the {@code Driver} is closed.
     * <p>
     * Note: task cannot return null
     */
    private <T> Optional<T> tryWithLockUninterruptibly(Supplier<T> task)
    {
        return tryWithLock(0, TimeUnit.MILLISECONDS, false, task);
    }

    /**
     * Try to acquire the {@code exclusiveLock} with {@code timeout} and run a {@code task}.
     * If the {@code interruptOnClose} flag is set to {@code true} the {@code task} will be
     * interrupted if the {@code Driver} is closed.
     * <p>
     * Note: task cannot return null
     */
    private <T> Optional<T> tryWithLock(long timeout, TimeUnit unit, boolean interruptOnClose, Supplier<T> task)
    {
        checkLockNotHeld("Lock cannot be reacquired");

        boolean acquired = false;
        try {
            acquired = exclusiveLock.tryLock(timeout, unit, interruptOnClose);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!acquired) {
            return Optional.empty();
        }

        T result = null;
        Throwable failure = null;

        try {
            result = task.get();

            // opportunistic check to avoid unnecessary lock reacquisition
            processNewSources();
            destroyIfNecessary();
        }
        catch (Throwable t) {
            failure = t;
        }
        finally {
            exclusiveLock.unlock();
        }

        // If there are more assignment updates available, attempt to reacquire the lock and process them.
        // This can happen if new assignments are added while we're holding the lock here doing work.
        // NOTE: this is separate duplicate code to make debugging lock reacquisition easier
        // The first condition is for processing the pending updates if this driver is still ALIVE
        // The second condition is to destroy the driver if the state is NEED_DESTRUCTION
        while (((pendingSplitAssignmentUpdates.get() != null && state.get() == State.ALIVE) || state.get() == State.NEED_DESTRUCTION)
                && exclusiveLock.tryLock(interruptOnClose)) {
            try {
                try {
                    processNewSources();
                }
                catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    }
                    else if (failure != t) {
                        failure.addSuppressed(t);
                    }
                }

                try {
                    destroyIfNecessary();
                }
                catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    }
                    else if (failure != t) {
                        failure.addSuppressed(t);
                    }
                }
            }
            finally {
                exclusiveLock.unlock();
            }
        }

        if (failure != null) {
            throwIfUnchecked(failure);
            // should never happen
            throw new AssertionError(failure);
        }

        verify(result != null, "result is null");
        return Optional.of(result);
    }

    private static class DriverLock
    {
        private final ReentrantLock lock = new ReentrantLock();

        private final AtomicReference<State> state;
        private final SettableFuture<Void> destroyedFuture;

        private DriverLock(AtomicReference<State> state, SettableFuture<Void> destroyedFuture)
        {
            this.state = requireNonNull(state, "state is null");
            this.destroyedFuture = requireNonNull(destroyedFuture, "destroyedFuture is null");
        }

        @GuardedBy("this")
        private Thread currentOwner;
        @GuardedBy("this")
        private boolean currentOwnerInterruptionAllowed;

        @GuardedBy("this")
        private List<StackTraceElement> interrupterStack;

        public boolean isHeldByCurrentThread()
        {
            return lock.isHeldByCurrentThread();
        }

        public boolean tryLock(boolean currentThreadInterruptionAllowed)
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock();
            if (acquired) {
                setOwner(currentThreadInterruptionAllowed);
            }
            return acquired;
        }

        public boolean tryLock(long timeout, TimeUnit unit, boolean currentThreadInterruptionAllowed)
                throws InterruptedException
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock(timeout, unit);
            if (acquired) {
                setOwner(currentThreadInterruptionAllowed);
            }
            return acquired;
        }

        private synchronized void setOwner(boolean interruptionAllowed)
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            currentOwner = Thread.currentThread();
            currentOwnerInterruptionAllowed = interruptionAllowed;
            // NOTE: We do not use interrupted stack information to know that another
            // thread has attempted to interrupt the driver, and interrupt this new lock
            // owner.  The interrupted stack information is for debugging purposes only.
            // In the case of interruption, the caller should (and does) have a separate
            // state to prevent further processing in the Driver.
        }

        public void unlock()
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            synchronized (this) {
                currentOwner = null;
                currentOwnerInterruptionAllowed = false;
            }
            lock.unlock();
            // Set the destroyed signal after releasing the lock since callbacks are fired synchronously and
            // otherwise could cause a deadlock
            if (state.get() == State.DESTROYED) {
                destroyedFuture.set(null);
            }
        }

        public synchronized List<StackTraceElement> getInterrupterStack()
        {
            return interrupterStack;
        }

        public synchronized void interruptCurrentOwner()
        {
            if (!currentOwnerInterruptionAllowed) {
                return;
            }
            // there is a benign race condition here were the lock holder
            // can be change between attempting to get lock and grabbing
            // the synchronized lock here, but in either case we want to
            // interrupt the lock holder thread
            if (interrupterStack == null) {
                interrupterStack = ImmutableList.copyOf(Thread.currentThread().getStackTrace());
            }

            if (currentOwner != null) {
                currentOwner.interrupt();
            }
        }
    }
}
