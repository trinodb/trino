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
package io.trino.spi.function;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates a parameter or return value can be NULL.
 * <p>
 * {@code @SqlNullable} annotation, when placed on a parameter of a {@link ScalarFunction} or {@link ScalarOperator} method,
 * indicates that the method should be called also when the parameter is NULL. In the absence of {@code @SqlNullable}
 * annotation, the engine assumes the implemented SQL function or operator is supposed to return NULL when given parameter is NULL
 * and does not call the implementing method.
 * <p>
 * {@code @SqlNullable} placed on a {@link ScalarFunction} method indicates that the implementation may return NULL.
 */
@Retention(RUNTIME)
@Target({METHOD, PARAMETER})
public @interface SqlNullable
{
    String value() default "";
}
