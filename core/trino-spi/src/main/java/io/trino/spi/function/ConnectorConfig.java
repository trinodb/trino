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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConnectorConfig
{
    /**
     * Connector label
     */
    String connectorLabel() default "";

    /**
     * Whether to enable the connector properties configuration panel or not
     */
    boolean propertiesEnabled() default false;

    /**
     * Whether to enable properties file upload for current connector configuration or not
     */
    boolean catalogConfigFilesEnabled() default false;

    /**
     * Whether to enable properties file upload for system configuration (system configuration are shared by all connectors) or not
     */
    boolean globalConfigFilesEnabled() default false;

    /**
     * Link to official introduction document of the connector
     */
    String docLink() default "";

    /**
     * Link to official configuration document of the connector
     */
    String configLink() default "";
}
