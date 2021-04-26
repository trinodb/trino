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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class Program
{
    private final List<Instruction> instructions;
    private final int minSlotCount;
    private final int minLabelCount;

    public Program(List<Instruction> instructions)
    {
        this.instructions = ImmutableList.copyOf(instructions);
        this.minSlotCount = toIntExact(instructions.stream()
                .filter(instruction -> instruction.type() == Instruction.Type.SAVE)
                .count());
        this.minLabelCount = toIntExact(instructions.stream()
                .filter(instruction -> instruction.type() == Instruction.Type.MATCH_LABEL)
                .count());
    }

    public Instruction at(int pointer)
    {
        return instructions.get(pointer);
    }

    public int size()
    {
        return instructions.size();
    }

    public List<Instruction> getInstructions()
    {
        return ImmutableList.copyOf(instructions);
    }

    public int getMinSlotCount()
    {
        return minSlotCount;
    }

    public int getMinLabelCount()
    {
        return minLabelCount;
    }

    public String dump()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("Min slots: ")
                .append(minSlotCount)
                .append("\n")
                .append("Min labels: ")
                .append(minLabelCount)
                .append("\n");
        for (int i = 0; i < instructions.size(); i++) {
            builder.append(format("%s: %s\n", i, instructions.get(i)));
        }

        return builder.toString();
    }
}
