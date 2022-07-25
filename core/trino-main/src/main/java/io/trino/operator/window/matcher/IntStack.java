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

import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

class IntStack
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntStack.class).instanceSize();

    private int[] values;
    private int next;

    public IntStack(int capacity)
    {
        values = new int[capacity];
    }

    public void push(int value)
    {
        ensureCapacity();
        values[next] = value;
        next++;
    }

    public int pop()
    {
        next--;
        return values[next];
    }

    public int size()
    {
        return next;
    }

    private void ensureCapacity()
    {
        if (next == values.length) {
            values = Arrays.copyOf(values, next * 2 + 1);
        }
    }

    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(values);
    }
}
