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

package io.trino.spi.queryeditorui;

import io.trino.spi.function.ConnectorConfig;
import io.trino.spi.function.Mandatory;

import java.lang.reflect.Method;
import java.util.Optional;

public class ConnectorUtil
{
    private ConnectorUtil()
    {
    }

    public static Optional<ConnectorWithProperties> assembleConnectorProperties(ConnectorConfig connectorConfig, Iterable<Method> methods)
    {
        if (connectorConfig == null) {
            return Optional.empty();
        }
        else {
            ConnectorWithProperties connectorWithProperties = new ConnectorWithProperties();
            connectorWithProperties.setConnectorLabel(Optional.of(connectorConfig.connectorLabel()));
            connectorWithProperties.setPropertiesEnabled(connectorConfig.propertiesEnabled());
            connectorWithProperties.setCatalogConfigFilesEnabled(connectorConfig.catalogConfigFilesEnabled());
            connectorWithProperties.setGlobalConfigFilesEnabled(connectorConfig.globalConfigFilesEnabled());
            connectorWithProperties.setDocLink(connectorConfig.docLink());
            connectorWithProperties.setConfigLink(connectorConfig.configLink());

            for (Method method : methods) {
                ConnectorWithProperties.Properties properties = new ConnectorWithProperties.Properties();
                Mandatory mandatory = method.getAnnotation(Mandatory.class);
                if (mandatory != null) {
                    properties.setName(mandatory.name());
                    properties.setDescription(mandatory.description());
                    properties.setValue(mandatory.defaultValue());
                    properties.setRequired(Optional.of(mandatory.required()));
                    properties.setReadOnly(Optional.of(mandatory.readOnly()));
                    properties.setType(Optional.of(mandatory.type().stringValue()));
                    connectorWithProperties.addProperties(properties);
                }
            }
            return Optional.of(connectorWithProperties);
        }
    }

    public static void addConnUrlProperty(Optional<ConnectorWithProperties> connectorWithProperties, String conUrl)
    {
        if (connectorWithProperties.isPresent()) {
            ConnectorWithProperties.Properties properties = new ConnectorWithProperties.Properties();
            properties.setName("connection-url");
            properties.setDescription("The connection URL of remote database");
            properties.setValue(conUrl);
            properties.setRequired(Optional.of(true));
            properties.setReadOnly(Optional.of(false));
            properties.setType(Optional.of(PropertyType.STRING.stringValue()));
            connectorWithProperties.get().addProperties(properties);
        }
    }
}
