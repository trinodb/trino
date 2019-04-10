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
package io.prestosql.server.extension.query.history;

import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.prestosql.server.extension.ExtensionFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class QueryHistoryStoreFactory
{
    public static final String QUERY_HISTORY_EXTENSION_CONFIG_FILE = "etc/query-history-store.properties";

    private static final Logger log = Logger.get(QueryHistoryStoreFactory.class);

    private static final Optional<QueryHistoryStore> binding = loadQueryHistoryExtension();

    private QueryHistoryStoreFactory() {}

    private static Optional<QueryHistoryStore> loadQueryHistoryExtension()
    {
        Properties extensionProps;
        try {
            extensionProps = getExtensionConf();
        }
        catch (IOException e) {
            log.warn("Failed to load query extension config from " + QUERY_HISTORY_EXTENSION_CONFIG_FILE);
            return Optional.empty();
        }
        if (extensionProps == null) {
            return Optional.empty();
        }
        // The implementation class is defined as a property `com.facebook.presto.server.extension.query.history.QueryHistoryStore.impl`.
        String extensionImplClass = extensionProps.getProperty(QueryHistoryStore.class.getName() + ".impl");
        if (Strings.isNullOrEmpty(extensionImplClass)) {
            return Optional.empty();
        }
        Optional<? extends QueryHistoryStore> extension = ExtensionFactory.INSTANCE.createExtension(extensionImplClass, extensionProps, QueryHistoryStore.class);
        if (extension.isPresent()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    extension.get().close();
                }
                catch (IOException e) {
                    log.error("Failed to close query history store", e);
                }
            }));
        }
        return (Optional<QueryHistoryStore>) extension;
    }

    private static Properties getExtensionConf() throws IOException
    {
        File extensionPropsFile = new File(QUERY_HISTORY_EXTENSION_CONFIG_FILE);
        if (extensionPropsFile.exists()) {
            Properties config = new Properties();
            try (InputStream configResource = new FileInputStream(extensionPropsFile)) {
                config.load(configResource);
            }
            return config;
        }
        return null;
    }

    public static Optional<QueryHistoryStore> getQueryHistoryStore()
    {
        return binding;
    }
}
