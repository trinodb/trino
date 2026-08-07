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
package io.trino.testing.containers.environment;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares which {@link ProductTestEnvironment} a test class requires.
 * <p>
 * The {@link ProductTestExtension} reads this annotation and ensures the
 * specified environment is running before any tests in the class execute.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * @ProductTest
 * @RequiresEnvironment(MySqlEnvironment.class)
 * class TestMySqlQueries {
 *     // ...
 * }
 * }</pre>
 * <p>
 * This annotation is {@link Inherited}, so subclasses automatically inherit
 * the parent's environment requirement. Subclasses can override by specifying
 * their own {@code @RequiresEnvironment}.
 * <p>
 * <b>Abstract base class pattern:</b>
 * <pre>{@code
 * // Define tests once
 * abstract class AbstractLdapTests {
 *     @Test
 *     void testLdapAuth(ProductTestEnvironment env) { ... }
 * }
 *
 * // Run in different environments
 * @ProductTest
 * @RequiresEnvironment(LdapEnvironment.class)
 * class TestLdapWithBasicAuth extends AbstractLdapTests {}
 *
 * @ProductTest
 * @RequiresEnvironment(LdapWithSslEnvironment.class)
 * class TestLdapWithSsl extends AbstractLdapTests {}
 * }</pre>
 *
 * @see ProductTest
 * @see ProductTestEnvironment
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface RequiresEnvironment
{
    /**
     * The environment class that this test requires.
     * Must have a public no-arg constructor.
     */
    Class<? extends ProductTestEnvironment> value();
}
