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
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a scalar function as a non-static method, invocable via SQL:2023
 * {@code <method invocation>} syntax: {@code receiver.method(args)}.
 * The receiver type is taken from the first {@code @SqlType} argument of
 * the implementation, which becomes the {@code self} parameter.
 */
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface InstanceMethod {}
