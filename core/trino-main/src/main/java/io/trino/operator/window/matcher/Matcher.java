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
package io.trino.operator.window.matcher;

import io.trino.operator.window.pattern.LabelEvaluator;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.window.matcher.MatchResult.NO_MATCH;

public class Matcher
{
    private final Program program;

    private static class Runtime
    {
        private int step;

        private final int[] threads;
        private final IntStack freeThreadIds;
        private final int inputLength;
        private final boolean matchingAtPartitionStart;
        private final Captures captures;

        // For each instruction of the program, track if it was processed, and in which step.
        private final int[] steps;

        public Runtime(Program program, int inputLength, boolean matchingAtPartitionStart)
        {
            int size = program.size();
            threads = new int[2 * size];
            freeThreadIds = new IntStack(threads.length);
            for (int i = threads.length - 1; i >= 0; i--) {
                freeThreadIds.push(i);
            }
            this.captures = new Captures(threads.length, program.getMinSlotCount(), program.getMinLabelCount());
            this.inputLength = inputLength;
            this.steps = new int[size];
            this.matchingAtPartitionStart = matchingAtPartitionStart;
        }

        public void nextStep()
        {
            step++;
        }

        public boolean hasCapacity(int size)
        {
            return freeThreadIds.size() >= size;
        }

        private int forkThread(int parent)
        {
            int threadId = freeThreadIds.pop();
            captures.copy(parent, threadId);
            return threadId;
        }

        private int newThread()
        {
            int threadId = freeThreadIds.pop();
            captures.allocate(threadId);
            return threadId;
        }

        private void killThread(int threadId)
        {
            freeThreadIds.push(threadId);
            captures.release(threadId);
        }
    }

    public Matcher(Program program)
    {
        this.program = program;
    }

    public MatchResult run(LabelEvaluator labelEvaluator)
    {
        IntList current = new IntList(program.size());
        IntList next = new IntList(program.size());

        int inputLength = labelEvaluator.getInputLength();
        boolean matchingAtPartitionStart = labelEvaluator.isMatchingAtPartitionStart();

        Runtime runtime = new Runtime(program, inputLength, matchingAtPartitionStart);
        // increase the step number to avoid shortcut in `advanceAndSchedule`.
        runtime.nextStep();
        advanceAndSchedule(current, runtime.newThread(), 0, 0, runtime);

        MatchResult result = NO_MATCH;

        for (int index = 0; index < inputLength; index++) {
            if (current.size() == 0) {
                // no match found -- all threads are dead
                break;
            }
            checkState(runtime.hasCapacity(current.size()), "thread capacity insufficient");
            runtime.nextStep();
            boolean matched = false;
            // For every existing thread, consume the label if possible. Otherwise, kill the thread.
            // After consuming the label, advance to the next `MATCH_LABEL`. Collect the advanced threads in `next`,
            // which will be the starting point for the next iteration.
            for (int i = 0; i < current.size(); i++) {
                int threadId = current.get(i);
                int pointer = runtime.threads[threadId];
                Instruction instruction = program.at(pointer);
                switch (instruction.type()) {
                    case MATCH_LABEL:
                        int label = ((MatchLabel) instruction).getLabel();
                        if (labelEvaluator.evaluateLabel(label, runtime.captures.getLabels(threadId))) {
                            runtime.captures.saveLabel(threadId, label);
                            advanceAndSchedule(next, threadId, pointer + 1, index + 1, runtime);
                        }
                        else {
                            runtime.killThread(threadId);
                        }
                        break;
                    case DONE:
                        matched = true;
                        result = new MatchResult(true, runtime.captures.getLabels(threadId), runtime.captures.getCaptures(threadId));
                        runtime.killThread(threadId);
                        break;
                    default:
                        throw new UnsupportedOperationException("not yet implemented");
                }
                if (matched) {
                    // do not process the following threads, because they are on less preferred paths than the match found
                    for (int j = i + 1; j < current.size(); j++) {
                        runtime.killThread(current.get(j));
                    }
                    break;
                }
            }
            IntList temp = current;
            temp.clear();
            current = next;
            next = temp;
        }

        // handle the case when the program still has instructions to process after consuming the whole input
        for (int i = 0; i < current.size(); i++) {
            int threadId = current.get(i);
            if (program.at(runtime.threads[threadId]).type() == Instruction.Type.DONE) {
                result = new MatchResult(true, runtime.captures.getLabels(threadId), runtime.captures.getCaptures(threadId));
                break;
            }
        }

        return result;
    }

    /**
     * For a particular thread identified by `threadId`, process consecutive instructions of the program,
     * from the instruction at `pointer` up to the next instruction which consumes a label or to the program end.
     * The resulting thread state (the pointer of the first not processed instruction) is recorded in `next`.
     * There might be multiple threads recorded in `next`, as a result of the instruction `SPLIT`.
     */
    private void advanceAndSchedule(IntList next, int threadId, int pointer, int inputIndex, Runtime runtime)
    {
        // avoid empty loop
        if (runtime.steps[pointer] == runtime.step) {
            // thread already exists at this point for the current input, so no need to schedule a new one
            runtime.killThread(threadId);
            return;
        }

        runtime.steps[pointer] = runtime.step;

        Instruction instruction = program.at(pointer);
        switch (instruction.type()) {
            case MATCH_START:
                if (inputIndex == 0 && runtime.matchingAtPartitionStart) {
                    advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
                }
                else {
                    runtime.killThread(threadId);
                }
                break;
            case MATCH_END:
                if (inputIndex == runtime.inputLength) {
                    advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
                }
                else {
                    runtime.killThread(threadId);
                }
                break;
            case JUMP:
                advanceAndSchedule(next, threadId, ((Jump) instruction).getTarget(), inputIndex, runtime);
                break;
            case SPLIT:
                int forked = runtime.forkThread(threadId);
                advanceAndSchedule(next, threadId, ((Split) instruction).getFirst(), inputIndex, runtime);
                advanceAndSchedule(next, forked, ((Split) instruction).getSecond(), inputIndex, runtime);
                break;
            case SAVE:
                runtime.captures.save(threadId, inputIndex);
                advanceAndSchedule(next, threadId, pointer + 1, inputIndex, runtime);
                break;
            default:
                runtime.threads[threadId] = pointer;
                next.add(threadId);
                break;
        }
    }
}
