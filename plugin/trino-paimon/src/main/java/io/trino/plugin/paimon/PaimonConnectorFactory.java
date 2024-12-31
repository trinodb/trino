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
package io.trino.plugin.paimon;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Trino {@link ConnectorFactory}.
 */
public class PaimonConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(PaimonConnectorFactory.class);

    // see https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties
    private static final String HADOOP_CONF_FILES_KEY = "hive.config.resources";
    // see org.apache.paimon.utils.HadoopUtils
    private static final String HADOOP_CONF_PREFIX = "hadoop.";

    private static void readHadoopXml(String path, Map<String, String> config)
            throws Exception
    {
        path = path.trim();
        if (path.isEmpty()) {
            return;
        }

        File xmlFile = new File(path);
        NodeList propertyNodes =
                DocumentBuilderFactory.newInstance()
                        .newDocumentBuilder()
                        .parse(xmlFile)
                        .getElementsByTagName("property");
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == 1) {
                Element propertyElement = (Element) propertyNode;
                String key = propertyElement.getElementsByTagName("name").item(0).getTextContent();
                String value =
                        propertyElement.getElementsByTagName("value").item(0).getTextContent();
                if (!StringUtils.isNullOrWhitespaceOnly(value)) {
                    config.putIfAbsent(HADOOP_CONF_PREFIX + key, value);
                }
            }
        }
    }

    @Override
    public String getName()
    {
        return "paimon";
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return create(catalogName, config, context, new EmptyModule());
    }

    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module)
    {
        config = new HashMap<>(config);
        if (config.containsKey(HADOOP_CONF_FILES_KEY)) {
            for (String hadoopXml : config.get(HADOOP_CONF_FILES_KEY).split(",")) {
                try {
                    readHadoopXml(hadoopXml, config);
                }
                catch (Exception e) {
                    LOG.warn(
                            "Failed to read hadoop xml file " + hadoopXml + ", skipping this file.",
                            e);
                }
            }
        }

        ClassLoader classLoader = PaimonConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app =
                    new Bootstrap(
                            new JsonModule(),
                            new PaimonModule(config),
                            // We need hdfs to access local files and hadoop file system
                            new HdfsModule(),
                            new HdfsAuthenticationModule(),
                            // bind the trino file system module
                            new PaimonFileSystemModule(catalogName, context, new Options(config)),
                            binder -> {
                                binder.bind(ClassLoader.class)
                                        .toInstance(PaimonConnectorFactory.class.getClassLoader());
                                binder.bind(NodeVersion.class)
                                        .toInstance(
                                                new NodeVersion(
                                                        context.getNodeManager()
                                                                .getCurrentNode()
                                                                .getVersion()));
                                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                                binder.bind(OpenTelemetry.class)
                                        .toInstance(context.getOpenTelemetry());
                                binder.bind(Tracer.class).toInstance(context.getTracer());
                                binder.bind(OrcReaderConfig.class)
                                        .toInstance(new OrcReaderConfig());
                                binder.bind(CatalogName.class)
                                        .toInstance(new CatalogName(catalogName));
                            },
                            module);

            Injector injector =
                    app.doNotInitializeLogging()
                            .setRequiredConfigurationProperties(Map.of())
                            .setOptionalConfigurationProperties(config)
                            .initialize();

            return injector.getInstance(PaimonConnector.class);
        }
    }

    /**
     * Empty module for paimon connector factory.
     */
    public static class EmptyModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}
    }

    private static class PaimonFileSystemModule
            extends AbstractConfigurationAwareModule
    {
        private final String catalogName;
        private final NodeManager nodeManager;
        private final OpenTelemetry openTelemetry;
        private final Options config;

        public PaimonFileSystemModule(
                String catalogName, ConnectorContext context, Options config)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.nodeManager = context.getNodeManager();
            this.openTelemetry = context.getOpenTelemetry();
            this.config = config;
        }

        @Override
        protected void setup(Binder binder)
        {
            boolean metadataCacheEnabled =
                    Optional.ofNullable(config.get("metadata-cache.enabled"))
                            .map(Boolean::parseBoolean)
                            .orElse(false);
            install(
                    new FileSystemModule(
                            catalogName, nodeManager, openTelemetry, metadataCacheEnabled));
        }
    }
}
