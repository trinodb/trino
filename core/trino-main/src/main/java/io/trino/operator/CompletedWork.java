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

import jakarta.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public final class CompletedWork<T>
        implements Work<T>
{
    @Nullable
    private final T result;

    public CompletedWork()
    {
        this.result = null;
    }

    public CompletedWork(T value)
    {
        this.result = requireNonNull(value);
    }

    @Override
    public boolean process()
    {
        return true;
    }

    @Nullable
    @Override
    public T getResult()
    {
        return result;
    }
}
