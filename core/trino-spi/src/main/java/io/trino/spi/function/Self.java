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

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a single {@code @SqlType} parameter of a {@link ScalarFunction} as the
 * receiver ({@code self}) of an instance method. Unlike {@link InstanceMethod},
 * which always treats the first declared argument as the receiver, {@code @Self}
 * lets any argument be the receiver: {@code ends_with(string, suffix)} with
 * {@code @Self} on {@code string} is invocable both as the ordinary function
 * {@code ends_with(string, suffix)} and as the method {@code string.ends_with(suffix)}.
 * <p>
 * A function annotated with {@code @Self} is registered in both forms from a
 * single declaration; no separate wrapper method is required. {@code @Self} is
 * mutually exclusive with {@link InstanceMethod} and {@link StaticMethod}, and
 * may appear on at most one parameter.
 */
@Retention(RUNTIME)
@Target(PARAMETER)
public @interface Self {}
