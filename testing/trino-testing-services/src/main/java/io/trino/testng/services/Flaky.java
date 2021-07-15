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
package io.trino.testng.services;

import org.intellij.lang.annotations.Language;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.regex.Matcher;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @deprecated Use of this is strongly discouraged
 */
@Deprecated
@Retention(RUNTIME)
@Target(METHOD)
public @interface Flaky
{
    String issue();

    /**
     * A test is retried when it fails with a stacktrace, which string representation matches given regular expression.
     * The pattern is searched for, as if {@link Matcher#find()} was used.
     */
    @Language("RegExp")
    String match();
}
