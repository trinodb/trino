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
package io.trino.execution;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStateMachine
{
    private enum State
    {
        BREAKFAST, LUNCH, DINNER
    }

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testNullState()
    {
        assertThatThrownBy(() -> new StateMachine<>("test", executor, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("initialState is null");

        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST);

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.set(null))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.trySet(null))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.compareAndSet(State.BREAKFAST, null))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.compareAndSet(State.LUNCH, null))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.setIf(null, currentState -> true))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));

        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.setIf(null, currentState -> false))
                        .isInstanceOf(NullPointerException.class)
                        .hasMessage("newState is null"));
    }

    @Test
    public void testSet()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST, ImmutableSet.of(State.DINNER));
        assertThat(stateMachine.get()).isEqualTo(State.BREAKFAST);

        assertNoStateChange(stateMachine, () -> assertThat(stateMachine.set(State.BREAKFAST)).isEqualTo(State.BREAKFAST));

        assertStateChange(stateMachine, () -> assertThat(stateMachine.set(State.LUNCH)).isEqualTo(State.BREAKFAST), State.LUNCH);

        assertStateChange(stateMachine, () -> assertThat(stateMachine.set(State.BREAKFAST)).isEqualTo(State.LUNCH), State.BREAKFAST);

        // transition to a final state
        assertStateChange(stateMachine, () -> assertThat(stateMachine.set(State.DINNER)).isEqualTo(State.BREAKFAST), State.DINNER);

        // attempt transition from a final state
        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.set(State.LUNCH))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("test cannot transition from DINNER to LUNCH"));
        assertNoStateChange(stateMachine, () -> stateMachine.set(State.DINNER));
    }

    @Test
    public void testTrySet()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST, ImmutableSet.of(State.DINNER));
        assertThat(stateMachine.get()).isEqualTo(State.BREAKFAST);

        assertNoStateChange(stateMachine, () -> assertThat(stateMachine.trySet(State.BREAKFAST)).isEqualTo(State.BREAKFAST));

        assertStateChange(stateMachine, () -> assertThat(stateMachine.trySet(State.LUNCH)).isEqualTo(State.BREAKFAST), State.LUNCH);

        assertStateChange(stateMachine, () -> assertThat(stateMachine.trySet(State.BREAKFAST)).isEqualTo(State.LUNCH), State.BREAKFAST);

        // transition to a final state
        assertStateChange(stateMachine, () -> assertThat(stateMachine.trySet(State.DINNER)).isEqualTo(State.BREAKFAST), State.DINNER);

        // attempt transition from a final state
        assertNoStateChange(stateMachine, () -> stateMachine.trySet(State.LUNCH));
        assertNoStateChange(stateMachine, () -> stateMachine.trySet(State.DINNER));
    }

    @Test
    public void testCompareAndSet()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST, ImmutableSet.of(State.DINNER));
        assertThat(stateMachine.get()).isEqualTo(State.BREAKFAST);

        // no match with new state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.DINNER, State.LUNCH));

        // match with new state
        assertStateChange(stateMachine,
                () -> stateMachine.compareAndSet(State.BREAKFAST, State.LUNCH),
                State.LUNCH);

        // no match with same state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.BREAKFAST, State.LUNCH));

        // match with same state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.LUNCH, State.LUNCH));

        // transition to a final state
        assertStateChange(stateMachine, () -> stateMachine.compareAndSet(State.LUNCH, State.DINNER), State.DINNER);

        // attempt transition from a final state
        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.compareAndSet(State.DINNER, State.LUNCH))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("test cannot transition from DINNER to LUNCH"));
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.DINNER, State.DINNER));
    }

    @Test
    public void testSetIf()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST, ImmutableSet.of(State.DINNER));
        assertThat(stateMachine.get()).isEqualTo(State.BREAKFAST);

        // false predicate with new state
        assertNoStateChange(stateMachine,
                () -> assertThat(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertThat(currentState).isEqualTo(State.BREAKFAST);
                    return false;
                })).isFalse());

        // true predicate with new state
        assertStateChange(stateMachine,
                () -> assertThat(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertThat(currentState).isEqualTo(State.BREAKFAST);
                    return true;
                })).isTrue(),
                State.LUNCH);

        // false predicate with same state
        assertNoStateChange(stateMachine,
                () -> assertThat(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertThat(currentState).isEqualTo(State.LUNCH);
                    return false;
                })).isFalse());

        // true predicate with same state
        assertNoStateChange(stateMachine,
                () -> assertThat(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertThat(currentState).isEqualTo(State.LUNCH);
                    return true;
                })).isFalse());

        // transition to a final state
        assertStateChange(stateMachine, () -> stateMachine.setIf(State.DINNER, currentState -> true), State.DINNER);

        // attempt transition from a final state
        assertNoStateChange(stateMachine, () ->
                assertThatThrownBy(() -> stateMachine.setIf(State.LUNCH, currentState -> true))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("test cannot transition from DINNER to LUNCH"));
        assertNoStateChange(stateMachine, () -> stateMachine.setIf(State.LUNCH, currentState -> false));
        assertNoStateChange(stateMachine, () -> stateMachine.setIf(State.DINNER, currentState -> true));
    }

    private static void assertStateChange(StateMachine<State> stateMachine, StateChanger stateChange, State expectedState)
            throws Exception
    {
        State initialState = stateMachine.get();
        ListenableFuture<State> futureChange = stateMachine.getStateChange(initialState);

        SettableFuture<State> listenerChange = addTestListener(stateMachine);

        stateChange.run();

        assertThat(stateMachine.get()).isEqualTo(expectedState);

        assertThat(futureChange.get(10, SECONDS)).isEqualTo(expectedState);
        assertThat(listenerChange.get(10, SECONDS)).isEqualTo(expectedState);

        // listeners should not be retained if we are in a terminal state
        boolean isTerminalState = stateMachine.isTerminalState(expectedState);
        if (isTerminalState) {
            assertThat(stateMachine.getStateChangeListeners()).isEmpty();
        }
    }

    private static void assertNoStateChange(StateMachine<State> stateMachine, StateChanger stateChange)
    {
        State initialState = stateMachine.get();
        ListenableFuture<State> futureChange = stateMachine.getStateChange(initialState);

        SettableFuture<State> listenerChange = addTestListener(stateMachine);

        // listeners should not be added if we are in a terminal state, but listener should fire
        boolean isTerminalState = stateMachine.isTerminalState(initialState);
        if (isTerminalState) {
            assertThat(stateMachine.getStateChangeListeners()).isEmpty();
        }

        stateChange.run();

        assertThat(stateMachine.get()).isEqualTo(initialState);

        // the future change will trigger if the state machine is in a terminal state
        // this is to prevent waiting for state changes that will never occur
        assertThat(futureChange.isDone()).isEqualTo(isTerminalState);
        futureChange.cancel(true);

        // test listener future only completes if the state actually changed
        assertThat(listenerChange.isDone()).isFalse();
        listenerChange.cancel(true);
    }

    private static SettableFuture<State> addTestListener(StateMachine<State> stateMachine)
    {
        State initialState = stateMachine.get();
        SettableFuture<Boolean> initialStateNotified = SettableFuture.create();
        SettableFuture<State> stateChanged = SettableFuture.create();
        Thread addingThread = Thread.currentThread();
        stateMachine.addStateChangeListener(newState -> {
            Thread callbackThread = Thread.currentThread();
            if (callbackThread == addingThread) {
                stateChanged.setException(new AssertionError("Listener was not called back on a different thread"));
                return;
            }

            if (newState == initialState) {
                initialStateNotified.set(true);
            }
            else {
                stateChanged.set(newState);
            }
        });

        assertThat(tryGetFutureValue(initialStateNotified, 10, SECONDS).isPresent()).withFailMessage("Initial state notification not fired").isTrue();

        return stateChanged;
    }

    private interface StateChanger
    {
        void run();
    }
}
