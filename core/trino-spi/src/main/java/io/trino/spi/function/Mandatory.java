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

import io.trino.spi.queryeditorui.PropertyType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Mandatory
{
    /**
     * Property name
     */
    String name() default "";

    /**
     * Property value
     */
    String defaultValue() default "";

    /**
     * Property description
     */
    String description() default "";

    /**
     * Property required
     */
    boolean required() default false;

    /**
     * If the read-only mode is enabled, the value modification is not supported
     */
    boolean readOnly() default false;

    /**
     * Property type
     */
    PropertyType type() default PropertyType.STRING;
}
