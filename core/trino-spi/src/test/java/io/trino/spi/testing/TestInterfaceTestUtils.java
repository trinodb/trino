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

import org.testng.annotations.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInterfaceTestUtils
{
    @Test
    public void testReportUnimplementedInterfaceMethod()
    {
        assertThatThrownBy(() -> InterfaceTestUtils.assertAllMethodsOverridden(Interface.class, Implementation.class))
                .isInstanceOf(AssertionError.class)
                .hasMessage("io.trino.spi.testing.TestInterfaceTestUtils$Implementation does not override " +
                        "[public default void io.trino.spi.testing.TestInterfaceTestUtils$Interface.foo(java.lang.String)]");
    }

    private interface Interface
    {
        default void foo(String s) {}
    }

    private static class Implementation
            implements Interface {}

    @Test
    public void testAcceptStaticMethod()
    {
        InterfaceTestUtils.assertAllMethodsOverridden(InterfaceWithStaticMethod.class, ImplementationOfInterfaceWithStaticMethod.class);
    }

    private interface InterfaceWithStaticMethod
    {
        default void foo(String s) {}

        static void staticMethod(String s) {}
    }

    private static class ImplementationOfInterfaceWithStaticMethod
            implements InterfaceWithStaticMethod
    {
        @Override
        public void foo(String s) {}
    }

    @Test
    public void testAcceptMultipleImplementedInterfaces()
    {
        InterfaceTestUtils.assertAllMethodsOverridden(Interface.class, ImplementationWithMultipleInterfaces.class);
    }

    private static class ImplementationWithMultipleInterfaces
            implements Cloneable, Interface, Serializable
    {
        @Override
        public void foo(String s) {}
    }

    @Test
    public void testAcceptAbstractClass()
    {
        InterfaceTestUtils.assertAllMethodsOverridden(AbstractClass.class, ImplementationOfAbstractClass.class);
    }

    private abstract static class AbstractClass
    {
        public void foo(String s) {}
    }

    private static class ImplementationOfAbstractClass
            extends AbstractClass
    {
        @Override
        public void foo(String s) {}
    }

    @Test
    public void testAssertProperForwardingMethodsAreCalledWithPrivateMethods()
    {
        InterfaceTestUtils.assertProperForwardingMethodsAreCalled(InterfaceWithPrivateMethod.class, ForwardingInterfaceWithPrivateMethod::new);
    }
}
