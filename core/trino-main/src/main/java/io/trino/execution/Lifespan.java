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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openjdk.jol.info.ClassLayout;

public class Lifespan
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Lifespan.class).instanceSize();

    private static final Lifespan TASK_WIDE = new Lifespan();

    public static Lifespan taskWide()
    {
        return TASK_WIDE;
    }

    private Lifespan()
    {
    }

    public boolean isTaskWide()
    {
        return true;
    }

    @JsonCreator
    public static Lifespan jsonCreator(String value)
    {
        return Lifespan.taskWide();
    }

    @Override
    @JsonValue
    public String toString()
    {
        return "TaskWide";
    }

    @Override
    public boolean equals(Object o)
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return 42;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
