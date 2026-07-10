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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorWithProperties
{
    @JsonProperty
    private String connectorName;

    @JsonProperty
    private Optional<String> connectorLabel;

    @JsonProperty
    private boolean propertiesEnabled;

    @JsonProperty
    private boolean catalogConfigFilesEnabled;

    @JsonProperty
    private boolean globalConfigFilesEnabled;

    @JsonProperty
    private List<Properties> connectorProperties = new ArrayList<>();

    private String docLink;

    private String configLink;

    public ConnectorWithProperties()
    {
    }

    public ConnectorWithProperties(
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("connectorLabel") Optional<String> connectorLabel,
            @JsonProperty("propertiesEnabled") boolean propertiesEnabled,
            @JsonProperty("catalogConfigFilesEnabled") boolean catalogConfigFilesEnabled,
            @JsonProperty("globalConfigFilesEnabled") boolean globalConfigFilesEnabled,
            @JsonProperty("properties") List<Properties> connectorProperties)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.connectorLabel = requireNonNull(connectorLabel, "connectorLabel is null");
        this.propertiesEnabled = propertiesEnabled;
        this.catalogConfigFilesEnabled = catalogConfigFilesEnabled;
        this.globalConfigFilesEnabled = globalConfigFilesEnabled;
        this.connectorProperties = requireNonNull(connectorProperties, "Properties is null");
    }

    public void setConnectorName(String connectorName)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
    }

    public void setConnectorLabel(Optional<String> connectorLabel)
    {
        this.connectorLabel = requireNonNull(connectorLabel, "connectorLabel is null");
    }

    public void setPropertiesEnabled(boolean propertiesEnabled)
    {
        this.propertiesEnabled = propertiesEnabled;
    }

    public void setCatalogConfigFilesEnabled(boolean catalogConfigFilesEnabled)
    {
        this.catalogConfigFilesEnabled = catalogConfigFilesEnabled;
    }

    public void setGlobalConfigFilesEnabled(boolean globalConfigFilesEnabled)
    {
        this.globalConfigFilesEnabled = globalConfigFilesEnabled;
    }

    public void addProperties(Properties properties)
    {
        this.connectorProperties.add(properties);
    }

    public void setDocLink(String docLink)
    {
        this.docLink = docLink;
    }

    public void setConfigLink(String configLink)
    {
        this.configLink = configLink;
    }

    public String getDocLink()
    {
        return docLink;
    }

    public String getConfigLink()
    {
        return configLink;
    }

    @JsonProperty
    public String getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public Optional<String> getConnectorLabel()
    {
        if (connectorLabel != null && connectorLabel.isPresent()) {
            return connectorLabel;
        }
        return Optional.of(connectorName);
    }

    @JsonProperty
    public boolean isPropertiesEnabled()
    {
        return propertiesEnabled;
    }

    @JsonProperty
    public boolean isCatalogConfigFilesEnabled()
    {
        return catalogConfigFilesEnabled;
    }

    @JsonProperty
    public boolean isGlobalConfigFilesEnabled()
    {
        return globalConfigFilesEnabled;
    }

    @JsonProperty
    public List<Properties> getConnectorProperties()
    {
        return connectorProperties;
    }

    public static class Properties
    {
        private String name;
        private String value;
        private String description;
        private Optional<String> type;
        private Optional<Boolean> required;
        private Optional<Boolean> readOnly;

        public Properties()
        {
        }

        @JsonCreator
        public Properties(
                @JsonProperty("name") String name,
                @JsonProperty("value") String value,
                @JsonProperty("description") String description,
                @JsonProperty("type") Optional<String> type,
                @JsonProperty("required") Optional<Boolean> required,
                @JsonProperty("readOnly") Optional<Boolean> readOnly)
        {
            this.name = name;
            this.value = value;
            this.description = description;
            this.type = type;
            this.required = required;
            this.readOnly = readOnly;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public void setValue(String value)
        {
            this.value = value;
        }

        public void setDescription(String description)
        {
            this.description = description;
        }

        public void setType(Optional<String> type)
        {
            this.type = type;
        }

        public void setRequired(Optional<Boolean> required)
        {
            this.required = required;
        }

        public void setReadOnly(Optional<Boolean> readOnly)
        {
            this.readOnly = readOnly;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getValue()
        {
            return value;
        }

        @JsonProperty
        public String getDescription()
        {
            return description;
        }

        @JsonProperty
        public Optional<String> getType()
        {
            return type;
        }

        @JsonProperty
        public Optional<Boolean> getRequired()
        {
            return required;
        }

        @JsonProperty
        public Optional<Boolean> getReadOnly()
        {
            return readOnly;
        }
    }
}
