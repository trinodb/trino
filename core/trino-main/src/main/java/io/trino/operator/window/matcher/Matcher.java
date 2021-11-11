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

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.window.pattern.LabelEvaluator;
import io.trino.operator.window.pattern.PhysicalValuePointer;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.trino.operator.window.matcher.MatchResult.NO_MATCH;

public class Matcher
{
    private final Program program;
    private final ThreadEquivalence threadEquivalence;

    private static class Runtime
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Runtime.class).instanceSize();

        // a helper structure for identifying equivalent threads
        // program pointer (instruction) --> list of threads that have reached this instruction
        private final IntMultimap threadsAtInstructions;

        private final IntList threads;
        private final IntStack freeThreadIds;
        private int newThreadId;
        private final int inputLength;
        private final boolean matchingAtPartitionStart;
        private final Captures captures;

        public Runtime(Program program, int inputLength, boolean matchingAtPartitionStart)
        {
            int initialCapacity = 2 * program.size();
            threads = new IntList(initialCapacity);
            freeThreadIds = new IntStack(initialCapacity);
            this.captures = new Captures(initialCapacity, program.getMinSlotCount(), program.getMinLabelCount());
            this.inputLength = inputLength;
            this.matchingAtPartitionStart = matchingAtPartitionStart;

            this.threadsAtInstructions = new IntMultimap(program.size(), program.size());
        }

        private int forkThread(int parent)
        {
            int child = newThread();
            captures.copy(parent, child);
            return child;
        }

        private int newThread()
        {
            if (freeThreadIds.size() > 0) {
                return freeThreadIds.pop();
            }
            return newThreadId++;
        }

        private void killThread(int threadId)
        {
            freeThreadIds.push(threadId);
            captures.release(threadId);
        }

        private long getSizeInBytes()
        {
            return INSTANCE_SIZE + threadsAtInstructions.getSizeInBytes() + threads.getSizeInBytes() + freeThreadIds.getSizeInBytes() + captures.getSizeInBytes();
        }
    }

    public Matcher(Program program, List<List<PhysicalValuePointer>> valuePointers)
    {
        this.program = program;
        this.threadEquivalence = new ThreadEquivalence(program, valuePointers);
    }

    public MatchResult run(LabelEvaluator labelEvaluator, LocalMemoryContext memoryContext)
    {
        IntList current = new IntList(program.size());
        IntList next = new IntList(program.size());

        int inputLength = labelEvaluator.getInputLength();
        boolean matchingAtPartitionStart = labelEvaluator.isMatchingAtPartitionStart();

        Runtime runtime = new Runtime(program, inputLength, matchingAtPartitionStart);

        advanceAndSchedule(current, runtime.newThread(), 0, 0, runtime);

        MatchResult result = NO_MATCH;

        for (int index = 0; index < inputLength; index++) {
            if (current.size() == 0) {
                // no match found -- all threads are dead
                break;
            }
            boolean matched = false;
            // For every existing thread, consume the label if possible. Otherwise, kill the thread.
            // After consuming the label, advance to the next `MATCH_LABEL`. Collect the advanced threads in `next`,
            // which will be the starting point for the next iteration.

            // clear the structure for new input index
            runtime.threadsAtInstructions.clear();

            for (int i = 0; i < current.size(); i++) {
                int threadId = current.get(i);
                int pointer = runtime.threads.get(threadId);
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

            // report memory usage. memory is not reported for constant structures: program, threadEquivalence
            memoryContext.setBytes(runtime.getSizeInBytes() + current.getSizeInBytes() + next.getSizeInBytes());

            IntList temp = current;
            temp.clear();
            current = next;
            next = temp;
        }

        // handle the case when the program still has instructions to process after consuming the whole input
        for (int i = 0; i < current.size(); i++) {
            int threadId = current.get(i);
            if (program.at(runtime.threads.get(threadId)).type() == Instruction.Type.DONE) {
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
        // avoid empty loop and try avoid exponential processing
        ArrayView threadsAtInstruction = runtime.threadsAtInstructions.getArrayView(pointer);
        for (int i = 0; i < threadsAtInstruction.length(); i++) {
            int thread = threadsAtInstruction.get(i);
            if (threadEquivalence.equivalent(thread, runtime.captures.getLabels(thread), threadId, runtime.captures.getLabels(threadId), pointer)) {
                // in case of equivalent threads, kill the one that comes later, because it is on a less preferred path
                runtime.killThread(threadId);
                return;
            }
        }
        runtime.threadsAtInstructions.add(pointer, threadId);

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
            default: // MATCH_LABEL or DONE
                runtime.threads.set(threadId, pointer);
                next.add(threadId);
                break;
        }
    }
}
