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
package io.trino.testing.services.junit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openjdk.jmh.annotations.Setup;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Consumer;

import static io.trino.testing.services.junit.ReportUnannotatedMethod.reportUnannotatedMethods;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReportUnannotatedMethod
{
    @Test
    public void testUnannotatedMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(UnannotatedMethod.class))
                .hasMessage("Test class io.trino.testing.services.junit.TestReportUnannotatedMethod$UnannotatedMethod has methods which JUnit does not invoke. Are they missing @Test? If not, make them private or protected.\n" +
                        "\tpublic void io.trino.testing.services.junit.TestReportUnannotatedMethod$UnannotatedMethod.testMissingAnnotation()");
    }

    @Test
    public void testInheritedUnannotatedMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(InheritsUnannotatedMethod.class))
                .hasMessageEndingWith("\n\tpublic void io.trino.testing.services.junit.TestReportUnannotatedMethod$UnannotatedMethod.testMissingAnnotation()");
    }

    @Test
    public void testOverridesAnnotatedMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(OverridesAnnotatedMethod.class))
                .hasMessageEndingWith("\n\tpublic void io.trino.testing.services.junit.TestReportUnannotatedMethod$OverridesAnnotatedMethod.test()");
    }

    @Test
    public void testOverridesInterfaceTestMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(OverridesInterfaceTestMethod.class))
                .hasMessageEndingWith("\n\tpublic void io.trino.testing.services.junit.TestReportUnannotatedMethod$OverridesInterfaceTestMethod.testFromInterface()");
    }

    @Test
    public void testUnannotatedInterfaceMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(ImplementsInterfaceWithUnannotatedMethod.class))
                .hasMessageEndingWith("\n\tpublic default void io.trino.testing.services.junit.TestReportUnannotatedMethod$InterfaceWithUnannotatedMethod.testMissingAnnotation()");
    }

    @Test
    public void testNonInvokingAnnotation()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(NonInvokingAnnotation.class))
                .hasMessageEndingWith("\n\tpublic void io.trino.testing.services.junit.TestReportUnannotatedMethod$NonInvokingAnnotation.testDisabledOnly()");
    }

    @Test
    public void testPackagePrivateMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(PackagePrivateMethod.class))
                .hasMessageEndingWith("\n\tvoid io.trino.testing.services.junit.TestReportUnannotatedMethod$PackagePrivateMethod.testMissingAnnotation()");
    }

    @Test
    public void testInheritedPackagePrivateMethod()
    {
        assertThatThrownBy(() -> reportUnannotatedMethods(InheritsPackagePrivateMethod.class))
                .hasMessageEndingWith("\n\tvoid io.trino.testing.services.junit.TestReportUnannotatedMethod$PackagePrivateMethod.testMissingAnnotation()");
    }

    @Test
    public void testAnnotatedMethods()
    {
        assertThatNoException().isThrownBy(() -> reportUnannotatedMethods(AnnotatedMethods.class));
    }

    @Test
    public void testPrivateAndProtectedMethods()
    {
        assertThatNoException().isThrownBy(() -> reportUnannotatedMethods(PrivateAndProtectedMethods.class));
    }

    @Test
    public void testContractMethods()
    {
        assertThatNoException().isThrownBy(() -> reportUnannotatedMethods(ContractMethods.class));
    }

    abstract static class UnannotatedMethod
    {
        @Test
        public void test() {}

        public void testMissingAnnotation() {}
    }

    abstract static class InheritsUnannotatedMethod
            extends UnannotatedMethod {}

    abstract static class AnnotatedMethods
    {
        @BeforeEach
        public void setUp() {}

        @Test
        public void test() {}

        @ParameterizedTest
        @ValueSource(ints = {1, 2})
        public void testParameterized(int value) {}

        @ComposedTest
        public void testComposed() {}

        @Setup
        public void setUpBenchmark() {}

        @AfterAll
        public static void tearDown() {}

        public static void helper() {}
    }

    abstract static class OverridesAnnotatedMethod
            extends AnnotatedMethods
    {
        @Override
        public void test() {}
    }

    interface InterfaceWithTest
    {
        @Test
        default void testFromInterface() {}
    }

    abstract static class OverridesInterfaceTestMethod
            implements InterfaceWithTest
    {
        @Override
        public void testFromInterface() {}
    }

    interface InterfaceWithUnannotatedMethod
    {
        @Test
        default void test() {}

        default void testMissingAnnotation() {}
    }

    abstract static class ImplementsInterfaceWithUnannotatedMethod
            implements InterfaceWithUnannotatedMethod {}

    abstract static class NonInvokingAnnotation
    {
        @Disabled
        public void testDisabledOnly() {}
    }

    abstract static class PackagePrivateMethod
    {
        @Test
        void test() {}

        void testMissingAnnotation() {}
    }

    abstract static class InheritsPackagePrivateMethod
            extends PackagePrivateMethod {}

    abstract static class PrivateAndProtectedMethods
    {
        @Test
        void test() {}

        @BeforeEach
        void setUp() {}

        protected void protectedHelper() {}

        static void staticHelper() {}
    }

    abstract static class ContractMethods
            implements AutoCloseable, Consumer<String>
    {
        @Test
        public void test() {}

        @Override
        public void close() {}

        @Override
        public void accept(String value) {}

        @Override
        public String toString()
        {
            return "ContractMethods";
        }
    }

    @Retention(RUNTIME)
    @Target(METHOD)
    @Test
    private @interface ComposedTest {}
}
