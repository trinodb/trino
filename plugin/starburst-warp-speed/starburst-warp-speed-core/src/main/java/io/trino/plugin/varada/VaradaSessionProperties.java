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
package io.trino.plugin.varada;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

@SuppressWarnings("ConstantConditions")
@Singleton
public final class VaradaSessionProperties
{
    public static final String EMPTY_QUERY = "empty_query";
    public static final String ENABLE_DEFAULT_WARMING = "enable_default_warming";
    public static final String ENABLE_OR_PUSHDOWN = "support_or";
    public static final String ENABLE_DEFAULT_WARMING_INDEX = "enable_default_warming_index";
    public static final String UNSUPPORTED_FUNCTIONS = "unsupported_functions";
    public static final String UNSUPPORTED_NATIVE_FUNCTIONS = "unsupported_native_functions";
    public static final String IMPORT_EXPORT_S3_PATH = "import_export_s3_path";
    public static final String IMPORT_EXPORT_RELATIVE_S3_PATH = "import_export";
    public static final String PREDICATE_SIMPLIFY_THRESHOLD = "simplify_predicate_threshold";
    public static final String SPLIT_TO_WORKER = "split_to_worker";
    public static final String MATCH_COLLECT_CATALOG = "match_collect_catalog";
    public static final String ENABLE_IMPORT_EXPORT = "enable_import_export";
    public static final String ENABLE_DICTIONARY = "enable_dictionary";
    public static final String ENABLE_MATCH_COLLECT = "enable_match_collect";
    public static final String ENABLE_MAPPED_MATCH_COLLECT = "enable_mapped_match_collect";
    public static final String ENABLE_INVERSE_WITH_NULLS = "enable_inverse_with_nulls";
    public static final String MIN_MAX_FILTER = "min_max_filter";

    private static final Logger logger = Logger.get(VaradaSessionProperties.class);
    private final List<PropertyMetadata<?>> sessionProperties;

    /**
     * MAKE SURE THAT IN CASES THAT THE SESSION PROPERTY IS OVERRIDING GLOBAL CONFIGURATION THE DEFAULT VALUE IS PROVIDED IN THE PROPERTY DEFINITION AND NOT AS STATIC METHOD
     *
     * @param globalConfiguration - global configuration
     */
    @Inject
    public VaradaSessionProperties(GlobalConfiguration globalConfiguration)
    {
        sessionProperties = List.of(
                booleanProperty(
                        EMPTY_QUERY,
                        "Execute the entire query without reading data, hence empty results",
                        false,
                        true),
                booleanProperty(
                        ENABLE_DEFAULT_WARMING,
                        "Warm query fields,with no WarmupRules",
                        globalConfiguration.isEnableDefaultWarming(),
                        true),
                booleanProperty(
                        ENABLE_DEFAULT_WARMING_INDEX,
                        "In default warming, warm as index if field was present in query",
                        globalConfiguration.isCreateIndexInDefaultWarming(),
                        true),
                booleanProperty(
                        ENABLE_IMPORT_EXPORT,
                        "export and import warmup files",
                        globalConfiguration.getEnableImportExport(),
                        true),
                booleanProperty(
                        ENABLE_DICTIONARY,
                        "dictionary feature enabled",
                        null,
                        true),
                booleanProperty(
                        ENABLE_MATCH_COLLECT,
                        "match collect feature enabled",
                        globalConfiguration.getEnableMatchCollect(),
                        true),
                booleanProperty(
                        ENABLE_MAPPED_MATCH_COLLECT,
                        "mapped match collect feature enabled",
                        globalConfiguration.getEnableMappedMatchCollect(),
                        true),
                booleanProperty(
                        ENABLE_INVERSE_WITH_NULLS,
                        "inverse with nulls feature enabled",
                        globalConfiguration.getEnableInverseWithNulls(),
                        true),
                integerProperty(
                        PREDICATE_SIMPLIFY_THRESHOLD,
                        "Max allowed predicate before simplifying",
                        globalConfiguration.getPredicateSimplifyThreshold(),
                        false),
                stringProperty(
                        UNSUPPORTED_FUNCTIONS,
                        "unsupported functions list. any function in the list will not be pushed down to workers. astrix will disable all",
                        null,
                        false),
                stringProperty(
                        UNSUPPORTED_NATIVE_FUNCTIONS,
                        "unsupported native functions list. any function in the list will not pushed down in native structure.",
                        null,
                        false),
                stringProperty(
                        IMPORT_EXPORT_S3_PATH,
                        "export and import warmup files",
                        null,
                        true),
                stringProperty(
                        SPLIT_TO_WORKER,
                        "pass splits to a specific worker number (for example 0, 1, 2), to a random worker in case of -1, or pass nodeIdentifier for a specific node",
                        null,
                        true),
                stringProperty(
                        MATCH_COLLECT_CATALOG,
                        "override default catalog for match collect. only one catalog can use it at a time.",
                        null,
                        true),
                booleanProperty(
                        ENABLE_OR_PUSHDOWN,
                        "support pushdown of OR predicates",
                        globalConfiguration.getEnableOrPushdown(),
                        true),
                booleanProperty(
                        MIN_MAX_FILTER,
                        "Use min and max values stored in warmUpElements for filtering",
                        true,
                        true));
    }

    public static Set<String> getUnsupportedNativeFunctions(
            ConnectorSession session,
            NativeConfiguration nativeConfiguration)
    {
        String properties = getProperty(session, UNSUPPORTED_NATIVE_FUNCTIONS, String.class);
        if (properties == null) {
            return nativeConfiguration.getUnsupportedNativeFunctions();
        }
        if (properties.isEmpty()) {
            return Collections.emptySet();
        }

        try {
            return Arrays.stream(properties.trim().split(",")).map(String::trim).collect(Collectors.toSet());
        }
        catch (Exception e) {
            logger.error("failed to parse session unsupported_functions properties, return default=%s",
                    nativeConfiguration.getUnsupportedNativeFunctions());
            return nativeConfiguration.getUnsupportedNativeFunctions();
        }
    }

    public static Set<String> getUnsupportedFunctions(ConnectorSession session, GlobalConfiguration globalConfiguration)
    {
        String properties = getProperty(session, UNSUPPORTED_FUNCTIONS, String.class);
        if (properties == null) {
            return globalConfiguration.getUnsupportedFunctions();
        }
        if (properties.isEmpty()) {
            return Collections.emptySet();
        }

        try {
            return Arrays.stream(properties.trim().split(",")).map(String::trim).collect(Collectors.toSet());
        }
        catch (Exception e) {
            logger.error("failed to parse session unsupported_functions properties, return default=%s", globalConfiguration.getUnsupportedFunctions());
            return globalConfiguration.getUnsupportedFunctions();
        }
    }

    public static boolean isEmptyQuery(ConnectorSession session)
    {
        if (session == null) {
            return false;
        }
        Boolean ret = getProperty(session, EMPTY_QUERY, Boolean.class);
        return ret != null ? ret : false;
    }

    public static Boolean isDefaultWarmingEnabled(ConnectorSession session)
    {
        return session == null ? null : getProperty(session, ENABLE_DEFAULT_WARMING, Boolean.class);
    }

    public static boolean isDefaultWarmingIndex(ConnectorSession session)
    {
        return getProperty(session, ENABLE_DEFAULT_WARMING_INDEX, Boolean.class);
    }

    public static Boolean getEnableImportExport(ConnectorSession session)
    {
        return session == null ? null : getProperty(session, ENABLE_IMPORT_EXPORT, Boolean.class);
    }

    public static Boolean getEnableDictionary(ConnectorSession session)
    {
        return session == null ? null : getProperty(session, ENABLE_DICTIONARY, Boolean.class);
    }

    public static boolean getEnabledMappedMatchCollect(ConnectorSession session)
    {
        Boolean ret = getProperty(session, ENABLE_MAPPED_MATCH_COLLECT, Boolean.class);
        return ret != null ? ret : false;
    }

    public static boolean getEnabledInverseWithNulls(ConnectorSession session)
    {
        Boolean ret = getProperty(session, ENABLE_INVERSE_WITH_NULLS, Boolean.class);
        return ret != null ? ret : false;
    }

    public static boolean getEnableMatchCollect(ConnectorSession session)
    {
        return getProperty(session, ENABLE_MATCH_COLLECT, Boolean.class);
    }

    public static boolean getEnableOrPushdown(ConnectorSession session)
    {
        Boolean res = getProperty(session, ENABLE_OR_PUSHDOWN, Boolean.class);
        return (res != null) ? res : false;
    }

    public static boolean isMinMaxFilter(ConnectorSession session)
    {
        Boolean res = getProperty(session, MIN_MAX_FILTER, Boolean.class);
        return (res != null) ? res : false;
    }

    public static String getS3ImportExportPath(
            ConnectorSession session,
            CloudVendorConfiguration configuration,
            CloudVendorService cloudVendorService)
    {
        if (session == null || getProperty(session, IMPORT_EXPORT_S3_PATH, String.class) == null) {
            return getS3ImportExportPath(configuration.getStorePath());
        }
        else {
            return getS3ImportExportPath(getProperty(session, IMPORT_EXPORT_S3_PATH, String.class));
        }
    }

    @VisibleForTesting
    static String getS3ImportExportPath(String s3StorePath)
    {
        return CloudVendorService.concatenatePath(s3StorePath, IMPORT_EXPORT_RELATIVE_S3_PATH);
    }

    public static int getPredicateSimplifyThreshold(
            ConnectorSession session,
            GlobalConfiguration globalConfiguration)
    {
        Object obj = getProperty(session, PREDICATE_SIMPLIFY_THRESHOLD, Integer.class);
        if (obj != null) {
            return (int) obj;
        }
        return globalConfiguration.getPredicateSimplifyThreshold();
    }

    public static String getMatchCollectCatalog(ConnectorSession session)
    {
        return getProperty(session, MATCH_COLLECT_CATALOG, String.class);
    }

    public static String getNodeByBySession(ConnectorSession session)
    {
        if (session == null) {
            return null;
        }
        return getProperty(session, SPLIT_TO_WORKER, String.class);
    }

    private static <T> T getProperty(ConnectorSession session, String name, Class<T> type)
    {
        try {
            return session.getProperty(name, type);
        }
        catch (TrinoException e) {
            logger.debug("session doesn't contains the property %s", name);
        }
        return null;
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
