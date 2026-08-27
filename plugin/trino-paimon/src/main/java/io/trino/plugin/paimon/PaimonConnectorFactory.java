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
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.utils.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.weakref.jmx.guice.MBeanModule;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class PaimonConnectorFactory
        implements ConnectorFactory
{
    private static final Logger LOG = Logger.get(PaimonConnectorFactory.class);

    // see
    // https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties
    private static final String HADOOP_CONF_FILES_KEY = "hive.config.resources";
    // see org.apache.paimon.utils.HadoopUtils
    private static final String HADOOP_CONF_PREFIX = "hadoop.";
    private static final String PAIMON_S3_ACCESS_KEY = "s3.access-key";
    private static final String PAIMON_S3_SECRET_KEY = "s3.secret-key";
    private static final String PAIMON_S3_ACCESS_KEY_FALLBACK = "s3.access.key";
    private static final String PAIMON_S3_SECRET_KEY_FALLBACK = "s3.secret.key";
    private static final String PAIMON_S3_PATH_STYLE_ACCESS = "s3.path-style-access";
    private static final String PAIMON_S3_PATH_STYLE_ACCESS_FALLBACK = "s3.path.style.access";
    private static final String PAIMON_S3_ENDPOINT = "s3.endpoint";
    private static final String PAIMON_S3_REGION = "s3.region";
    private static final String PAIMON_S3_ENDPOINT_REGION = "s3.endpoint.region";
    private static final String PAIMON_S3_SIGNER_TYPE = "s3.signer-type";
    private static final String PAIMON_S3_SIGNING_ALGORITHM = "s3.signing-algorithm";
    private static final String PAIMON_S3A_ACCESS_KEY = "s3a.access-key";
    private static final String PAIMON_S3A_SECRET_KEY = "s3a.secret-key";
    private static final String PAIMON_S3A_ACCESS_KEY_FALLBACK = "s3a.access.key";
    private static final String PAIMON_S3A_SECRET_KEY_FALLBACK = "s3a.secret.key";
    private static final String PAIMON_S3A_PATH_STYLE_ACCESS = "s3a.path-style-access";
    private static final String PAIMON_S3A_PATH_STYLE_ACCESS_FALLBACK = "s3a.path.style.access";
    private static final String PAIMON_S3A_ENDPOINT = "s3a.endpoint";
    private static final String PAIMON_S3A_REGION = "s3a.region";
    private static final String PAIMON_S3A_ENDPOINT_REGION = "s3a.endpoint.region";
    private static final String PAIMON_S3A_SIGNER_TYPE = "s3a.signer-type";
    private static final String PAIMON_S3A_SIGNING_ALGORITHM = "s3a.signing-algorithm";
    private static final String HADOOP_S3_ACCESS_KEY = "fs.s3a.access-key";
    private static final String HADOOP_S3_SECRET_KEY = "fs.s3a.secret-key";
    private static final String HADOOP_S3_ACCESS_KEY_FALLBACK = "fs.s3a.access.key";
    private static final String HADOOP_S3_SECRET_KEY_FALLBACK = "fs.s3a.secret.key";
    private static final String HADOOP_S3_PATH_STYLE_ACCESS = "fs.s3a.path-style-access";
    private static final String HADOOP_S3_PATH_STYLE_ACCESS_FALLBACK = "fs.s3a.path.style.access";
    private static final String HADOOP_S3_ENDPOINT = "fs.s3a.endpoint";
    private static final String HADOOP_S3_REGION = "fs.s3a.region";
    private static final String HADOOP_S3_ENDPOINT_REGION = "fs.s3a.endpoint.region";
    private static final String HADOOP_S3_SIGNER_TYPE = "fs.s3a.signer-type";
    private static final String HADOOP_S3_SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";
    private static final String TRINO_S3_ACCESS_KEY = "s3.aws-access-key";
    private static final String TRINO_S3_SECRET_KEY = "s3.aws-secret-key";

    private static final String DISALLOW_DOCTYPE_DECL = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

    static void readHadoopXml(String path, Map<String, String> config, Set<String> protectedConfigKeys)
            throws Exception
    {
        path = path.trim();
        if (path.isEmpty()) {
            return;
        }

        File xmlFile = new File(path);
        NodeList propertyNodes = newSecureDocumentBuilderFactory().newDocumentBuilder().parse(xmlFile)
                .getElementsByTagName("property");
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == 1) {
                Element propertyElement = (Element) propertyNode;
                Node nameNode = propertyElement.getElementsByTagName("name").item(0);
                Node valueNode = propertyElement.getElementsByTagName("value").item(0);
                if (nameNode == null || valueNode == null) {
                    continue;
                }
                String key = nameNode.getTextContent().trim();
                String value = valueNode.getTextContent();
                if (!key.isEmpty() && !StringUtils.isNullOrWhitespaceOnly(value)) {
                    String hadoopKey = HADOOP_CONF_PREFIX + key;
                    if (!protectedConfigKeys.contains(hadoopKey)) {
                        config.put(hadoopKey, value);
                    }
                }
            }
        }
    }

    private static DocumentBuilderFactory newSecureDocumentBuilderFactory()
            throws Exception
    {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        documentBuilderFactory.setFeature(DISALLOW_DOCTYPE_DECL, true);
        documentBuilderFactory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
        documentBuilderFactory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
        documentBuilderFactory.setFeature(LOAD_EXTERNAL_DTD, false);
        documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        documentBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);
        return documentBuilderFactory;
    }

    @Override
    public String getName()
    {
        return "paimon";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return create(catalogName, config, context, new EmptyModule());
    }

    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context, Module module)
    {
        config = new HashMap<>(config);
        addS3CredentialProperties(config);
        if (config.containsKey(HADOOP_CONF_FILES_KEY)) {
            Set<String> protectedConfigKeys = Set.copyOf(config.keySet());
            for (String hadoopXml : config.get(HADOOP_CONF_FILES_KEY).split(",")) {
                try {
                    readHadoopXml(hadoopXml, config, protectedConfigKeys);
                }
                catch (Exception e) {
                    LOG.warn(e, "Failed to read hadoop xml file %s, skipping this file.", hadoopXml);
                }
            }
        }

        ClassLoader classLoader = PaimonConnectorFactory.class.getClassLoader();
        verifyTrinoFormatFactories(classLoader);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule("org.apache.paimon.trino", "paimon.trino"),
                    new JsonModule(),
                    new TypeDeserializerModule(),
                    new PaimonModule(),
                    new MBeanServerModule(),
                    new FileSystemModule(catalogName, context, false),
                    new ConnectorContextModule(catalogName, context),
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .disableSystemProperties()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            PaimonMetadata paimonMetadata = injector.getInstance(PaimonMetadataFactory.class).create();
            PaimonSplitManager paimonSplitManager = injector.getInstance(PaimonSplitManager.class);
            PaimonPageSourceProvider paimonPageSourceProvider = injector.getInstance(PaimonPageSourceProvider.class);
            PaimonPageSinkProvider paimonPageSinkProvider = injector.getInstance(PaimonPageSinkProvider.class);
            PaimonNodePartitioningProvider paimonNodePartitioningProvider = injector
                    .getInstance(PaimonNodePartitioningProvider.class);
            PaimonSessionProperties paimonSessionProperties = injector.getInstance(PaimonSessionProperties.class);
            PaimonSchemaProperties paimonSchemaProperties = injector.getInstance(PaimonSchemaProperties.class);
            PaimonTableOptions paimonTableOptions = injector.getInstance(PaimonTableOptions.class);
            Set<ConnectorTableFunction> connectorTableFunctions = injector.getInstance(new Key<>() {});
            FunctionProvider functionProvider = injector.getInstance(FunctionProvider.class);

            return new PaimonConnector(
                    new ClassLoaderSafeConnectorMetadata(paimonMetadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(paimonSplitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(paimonPageSourceProvider, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(paimonPageSinkProvider, classLoader),
                    paimonNodePartitioningProvider,
                    paimonMetadata.catalog(),
                    paimonSchemaProperties,
                    paimonTableOptions,
                    paimonSessionProperties,
                    connectorTableFunctions,
                    functionProvider);
        }
    }

    /**
     * Verify that the Trino no-Hadoop format factories are discoverable for the
     * parquet and orc identifiers. If Paimon's native factories (which require
     * Hadoop) are also registered and would shadow the Trino factories, log a
     * warning so the deployment issue is visible before write failures occur.
     */
    private static void verifyTrinoFormatFactories(ClassLoader classLoader)
    {
        try {
            List<FileFormatFactory> factories = FactoryUtil.discoverFactories(classLoader, FileFormatFactory.class);
            Set<String> trinoIdentifiers = Set.of("parquet", "orc");
            Set<String> found = new HashSet<>();
            for (FileFormatFactory factory : factories) {
                found.add(factory.identifier());
                if (trinoIdentifiers.contains(factory.identifier())
                        && !factory.getClass().getName().startsWith("io.trino.")) {
                    LOG.warn("Native Paimon %s format factory %s is registered alongside "
                                    + "the Trino no-Hadoop factory. The paimon-bundle jar may not have "
                                    + "been stripped of conflicting service entries. Writes may fail "
                                    + "with ClassNotFoundException for Hadoop Configuration.",
                            factory.identifier(),
                            factory.getClass().getName());
                }
            }
            for (String expected : trinoIdentifiers) {
                if (!found.contains(expected)) {
                    LOG.warn("Trino no-Hadoop format factory for '%s' was not discovered. "
                            + "Check that the connector jar is on the plugin classpath.", expected);
                }
            }
        }
        catch (Throwable t) {
            LOG.warn(t, "Failed to verify Trino format factory registration");
        }
    }

    static void addS3CredentialProperties(Map<String, String> config)
    {
        copyIfMissingOrBlank(config, PAIMON_S3_ACCESS_KEY_FALLBACK, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3_SECRET_KEY_FALLBACK, PAIMON_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3_PATH_STYLE_ACCESS_FALLBACK, PAIMON_S3_PATH_STYLE_ACCESS);
        copyIfMissingOrBlank(config, PAIMON_S3_ENDPOINT_REGION, PAIMON_S3_REGION);
        copyIfMissingOrBlank(config, PAIMON_S3_SIGNING_ALGORITHM, PAIMON_S3_SIGNER_TYPE);
        copyIfMissingOrBlank(config, PAIMON_S3A_ACCESS_KEY, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3A_ACCESS_KEY_FALLBACK, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3A_SECRET_KEY, PAIMON_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3A_SECRET_KEY_FALLBACK, PAIMON_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3A_PATH_STYLE_ACCESS, PAIMON_S3_PATH_STYLE_ACCESS);
        copyIfMissingOrBlank(config, PAIMON_S3A_PATH_STYLE_ACCESS_FALLBACK, PAIMON_S3_PATH_STYLE_ACCESS);
        copyIfMissingOrBlank(config, PAIMON_S3A_ENDPOINT, PAIMON_S3_ENDPOINT);
        copyIfMissingOrBlank(config, PAIMON_S3A_REGION, PAIMON_S3_REGION);
        copyIfMissingOrBlank(config, PAIMON_S3A_ENDPOINT_REGION, PAIMON_S3_REGION);
        copyIfMissingOrBlank(config, PAIMON_S3A_SIGNER_TYPE, PAIMON_S3_SIGNER_TYPE);
        copyIfMissingOrBlank(config, PAIMON_S3A_SIGNING_ALGORITHM, PAIMON_S3_SIGNER_TYPE);
        copyIfMissingOrBlank(config, HADOOP_S3_ACCESS_KEY, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, HADOOP_S3_ACCESS_KEY_FALLBACK, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, HADOOP_S3_SECRET_KEY, PAIMON_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, HADOOP_S3_SECRET_KEY_FALLBACK, PAIMON_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, HADOOP_S3_PATH_STYLE_ACCESS, PAIMON_S3_PATH_STYLE_ACCESS);
        copyIfMissingOrBlank(config, HADOOP_S3_PATH_STYLE_ACCESS_FALLBACK, PAIMON_S3_PATH_STYLE_ACCESS);
        copyIfMissingOrBlank(config, HADOOP_S3_ENDPOINT, PAIMON_S3_ENDPOINT);
        copyIfMissingOrBlank(config, HADOOP_S3_REGION, PAIMON_S3_REGION);
        copyIfMissingOrBlank(config, HADOOP_S3_ENDPOINT_REGION, PAIMON_S3_REGION);
        copyIfMissingOrBlank(config, HADOOP_S3_SIGNER_TYPE, PAIMON_S3_SIGNER_TYPE);
        copyIfMissingOrBlank(config, HADOOP_S3_SIGNING_ALGORITHM, PAIMON_S3_SIGNER_TYPE);
        copyIfMissingOrBlank(config, PAIMON_S3_ACCESS_KEY, TRINO_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, PAIMON_S3_SECRET_KEY, TRINO_S3_SECRET_KEY);
        copyIfMissingOrBlank(config, TRINO_S3_ACCESS_KEY, PAIMON_S3_ACCESS_KEY);
        copyIfMissingOrBlank(config, TRINO_S3_SECRET_KEY, PAIMON_S3_SECRET_KEY);
    }

    private static void copyIfMissingOrBlank(Map<String, String> config, String sourceKey, String targetKey)
    {
        String value = config.get(sourceKey);
        if (!StringUtils.isNullOrWhitespaceOnly(value)
                && StringUtils.isNullOrWhitespaceOnly(config.get(targetKey))) {
            config.put(targetKey, value);
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
}
