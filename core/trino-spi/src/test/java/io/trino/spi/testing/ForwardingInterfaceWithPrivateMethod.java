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
package io.trino.spi.testing;

import static java.util.Objects.requireNonNull;

public class ForwardingInterfaceWithPrivateMethod
        implements InterfaceWithPrivateMethod
{
    // Declared as field for brevity. In a typical Forwarding class this would be delegate() abstract method
    private final InterfaceWithPrivateMethod delegate;

    public ForwardingInterfaceWithPrivateMethod(InterfaceWithPrivateMethod delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void foo()
    {
        delegate.foo();
    }

    @Override
    public void bar()
    {
        delegate.bar();
    }
}
