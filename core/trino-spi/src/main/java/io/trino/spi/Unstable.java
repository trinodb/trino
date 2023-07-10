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
package io.trino.spi;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Signifies that a public API (public class, method or field) is permanently subject to
 * incompatible changes, or even removal, in any future release, without prior notice. An API
 * bearing this annotation is exempt from any compatibility guarantees made by its containing
 * library. Note that the presence of this annotation implies nothing about the quality or
 * performance of the API in question, only the fact that it can evolve any time.
 */
@Retention(RUNTIME)
@Target({TYPE, FIELD, METHOD, CONSTRUCTOR})
@Documented
public @interface Unstable {}
