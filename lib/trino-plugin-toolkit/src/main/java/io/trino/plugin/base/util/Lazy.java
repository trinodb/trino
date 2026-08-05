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
package io.trino.plugin.base.util;

import com.google.common.base.Suppliers;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// A memoizing [java.util.function.Supplier].
/// Use this class when you want the type system to enforce memoization.
///
/// @see com.google.common.base.Suppliers#memoize
// This is not an interface to disallow implicit creation of Lazy with a lambda.
// It's believed that memoization should be visible in the code.
public final class Lazy<T>
        implements Supplier<T>
{
    public static <T> Lazy<T> from(Supplier<T> supplier)
    {
        return new Lazy<>(Suppliers.memoize(supplier::get));
    }

    private final Supplier<T> delegate;

    private Lazy(Supplier<T> delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public T get()
    {
        return delegate.get();
    }
}
