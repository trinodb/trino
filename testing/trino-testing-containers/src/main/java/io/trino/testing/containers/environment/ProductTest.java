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

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test class as a product test that uses managed environments.
 * <p>
 * This annotation registers the {@link ProductTestExtension} which handles:
 * <ul>
 *   <li>Environment lifecycle (start, reuse, shutdown)</li>
 *   <li>Single-environment-at-a-time constraint enforcement</li>
 *   <li>Parameter injection for {@link ProductTestEnvironment}</li>
 * </ul>
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
 * @ProductTest
 * @RequiresEnvironment(MySqlEnvironment.class)
 * class TestMySqlQueries {
 *
 *     @Test
 *     void testSelect(ProductTestEnvironment env) throws Exception {
 *         try (Connection conn = env.createTrinoConnection()) {
 *             // ...
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see RequiresEnvironment
 * @see ProductTestExtension
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ProductTestExtension.class)
public @interface ProductTest {}
