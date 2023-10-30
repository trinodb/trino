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
package io.trino.matching;

import java.util.NoSuchElementException;
import java.util.Objects;

public class Captures
{
    private static final Captures NIL = new Captures(null, null, null);

    private final Capture<?> capture;
    private final Object value;
    private final Captures tail;

    private Captures(Capture<?> capture, Object value, Captures tail)
    {
        this.capture = capture;
        this.value = value;
        this.tail = tail;
    }

    public static Captures empty()
    {
        return NIL;
    }

    public static <T> Captures ofNullable(Capture<T> capture, T value)
    {
        return capture == null ? empty() : new Captures(capture, value, NIL);
    }

    public Captures addAll(Captures other)
    {
        if (this == NIL) {
            return other;
        }
        return new Captures(capture, value, tail.addAll(other));
    }

    @SuppressWarnings("unchecked cast")
    public <T> T get(Capture<T> capture)
    {
        if (this.equals(NIL)) {
            throw new NoSuchElementException("Requested value for unknown Capture. Was it registered in the Pattern?");
        }
        if (this.capture.equals(capture)) {
            return (T) value;
        }
        return tail.get(capture);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Captures captures = (Captures) o;
        return Objects.equals(capture, captures.capture)
                && Objects.equals(value, captures.value)
                && Objects.equals(tail, captures.tail);
    }

    @Override
    public int hashCode()
    {
        int result = capture != null ? capture.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (tail != null ? tail.hashCode() : 0);
        return result;
    }
}
