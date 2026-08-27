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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class PaimonTableOptionUtils
{
    private static final Pattern CAMEL_CASE_BOUNDARY = Pattern.compile("([a-z0-9])([A-Z])");
    private static final Pattern OPTION_KEY_SEPARATOR = Pattern.compile("[.\\-]");
    private static final Set<String> RUNTIME_ONLY_TABLE_PROPERTY_OPTION_KEYS = Set.of(
            CoreOptions.SCAN_MODE.key(),
            CoreOptions.STREAM_SCAN_MODE.key(),
            CoreOptions.BATCH_SCAN_MODE.key(),
            CoreOptions.SCAN_TIMESTAMP.key(),
            CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
            CoreOptions.SCAN_WATERMARK.key(),
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_CREATION_TIME_MILLIS.key(),
            CoreOptions.SCAN_SNAPSHOT_ID.key(),
            CoreOptions.SCAN_TAG_NAME.key(),
            CoreOptions.SCAN_VERSION.key(),
            CoreOptions.SCAN_BOUNDED_WATERMARK.key(),
            CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
            CoreOptions.SCAN_MAX_SPLITS_PER_TASK.key(),
            CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(),
            CoreOptions.SCAN_IGNORE_LOST_FILE.key(),
            CoreOptions.SCAN_PLAN_AUTO_TAG_FOR_READ_TIME_RETAINED.key(),
            CoreOptions.INCREMENTAL_BETWEEN.key(),
            CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(),
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(),
            CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(),
            CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(),
            CoreOptions.STREAMING_READ_SNAPSHOT_DELAY.key(),
            CoreOptions.STREAMING_READ_OVERWRITE.key(),
            CoreOptions.STREAMING_READ_APPEND_OVERWRITE.key(),
            CoreOptions.CONSUMER_ID.key(),
            CoreOptions.CONSUMER_IGNORE_PROGRESS.key());
    private static final Set<String> EXCLUDED_FROM_DOCUMENTATION_OPTION_KEYS =
            excludedFromDocumentationOptionKeys(CoreOptions.class);
    private static final Set<String> EXCLUDED_TABLE_PROPERTY_OPTION_KEYS = Stream.concat(
                    RUNTIME_ONLY_TABLE_PROPERTY_OPTION_KEYS.stream(),
                    EXCLUDED_FROM_DOCUMENTATION_OPTION_KEYS.stream())
            .collect(toUnmodifiableSet());
    private static final Set<String> WRITE_DYNAMIC_OPTION_ONLY_KEYS = Set.of(
            CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(),
            CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(),
            CoreOptions.SCAN_FALLBACK_BRANCH.key(),
            CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key(),
            CoreOptions.SCAN_PRIMARY_BRANCH.key());
    private static final Set<String> EXCLUDED_TABLE_PROPERTY_TRINO_KEYS = EXCLUDED_TABLE_PROPERTY_OPTION_KEYS.stream()
            .map(PaimonTableOptionUtils::convertOptionKey)
            .collect(toUnmodifiableSet());
    private static final List<OptionInfo> OPTION_INFOS = buildOptionInfos();
    private static final Map<String, OptionInfo> ALL_OPTION_INFO_BY_PAIMON_KEY =
            indexOptionInfosByPaimonKey(buildAllOptionInfos());
    private static final Map<String, OptionInfo> OPTION_INFO_BY_TRINO_KEY = indexOptionInfosByTrinoKey(OPTION_INFOS);
    private static final Map<String, OptionInfo> OPTION_INFO_BY_PAIMON_KEY = indexOptionInfosByPaimonKey(OPTION_INFOS);

    private PaimonTableOptionUtils() {}

    public static void buildOptions(Schema.Builder builder, Map<String, Object> properties)
    {
        requireNonNull(builder, "builder is null");
        buildOptionMap(properties).forEach(builder::option);
    }

    static Map<String, String> buildOptionMap(Map<String, Object> properties)
    {
        requireNonNull(properties, "properties is null");
        Map<String, String> options = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = entry.getKey();
            validatePropertyKey(propertyName);
            OptionInfo optionInfo = OPTION_INFO_BY_TRINO_KEY.get(propertyName);
            if (optionInfo != null && entry.getValue() != null) {
                options.put(optionInfo.paimonOptionKey, normalizeOptionValue(optionInfo, entry.getValue()));
            }
        }
        return options;
    }

    static String normalizeOptionValue(String trinoOptionKey, String paimonOptionKey, Object rawValue)
    {
        requireNonNull(trinoOptionKey, "trinoOptionKey is null");
        requireNonNull(paimonOptionKey, "paimonOptionKey is null");
        OptionInfo optionInfo = OPTION_INFO_BY_TRINO_KEY.get(trinoOptionKey);
        if (optionInfo == null) {
            optionInfo = OPTION_INFO_BY_PAIMON_KEY.get(paimonOptionKey);
        }
        if (optionInfo == null) {
            return requireNonBlankStringOptionValue(trinoOptionKey, rawValue);
        }
        return normalizeOptionValue(optionInfo, rawValue);
    }

    static String normalizeDynamicOptionValue(String paimonOptionKey, String rawValue)
    {
        requireNonNull(paimonOptionKey, "paimonOptionKey is null");
        OptionInfo optionInfo = ALL_OPTION_INFO_BY_PAIMON_KEY.get(paimonOptionKey);
        if (optionInfo == null) {
            return requireNonBlankStringOptionValue(paimonOptionKey, rawValue);
        }
        return normalizeOptionValue(optionInfo, rawValue);
    }

    private static String normalizeOptionValue(OptionInfo optionInfo, Object rawValue)
    {
        String optionValue = requireNonBlankStringOptionValue(optionInfo.trinoOptionKey, rawValue);
        return optionInfo.valueRequiresTrim ? optionValue.trim() : optionValue;
    }

    static String requireNonBlankStringOptionValue(String propertyName, Object rawValue)
    {
        requireNonNull(propertyName, "propertyName is null");
        if (!(rawValue instanceof String optionValue)) {
            throw new IllegalArgumentException(
                    "properties value for property '%s' must be a string".formatted(propertyName));
        }
        if (StringUtils.isNullOrWhitespaceOnly(optionValue)) {
            throw new IllegalArgumentException(
                    "properties value for property '%s' is blank".formatted(propertyName));
        }
        return optionValue;
    }

    public static String toPaimonOptionKey(String trinoOptionKey)
    {
        requireNonNull(trinoOptionKey, "trinoOptionKey is null");
        if (StringUtils.isNullOrWhitespaceOnly(trinoOptionKey)) {
            throw new IllegalArgumentException("trinoOptionKey is blank");
        }
        OptionInfo optionInfo = OPTION_INFO_BY_TRINO_KEY.get(trinoOptionKey);
        return optionInfo != null ? optionInfo.paimonOptionKey : trinoOptionKey;
    }

    public static boolean isRuntimeOnlyTableProperty(String trinoOptionKey)
    {
        requireNonNull(trinoOptionKey, "trinoOptionKey is null");
        if (StringUtils.isNullOrWhitespaceOnly(trinoOptionKey)) {
            throw new IllegalArgumentException("trinoOptionKey is blank");
        }
        return EXCLUDED_TABLE_PROPERTY_TRINO_KEYS.contains(trinoOptionKey)
                || isRuntimeOnlyPaimonOptionKey(toPaimonOptionKey(trinoOptionKey));
    }

    static boolean isRuntimeOnlyPaimonOptionKey(String paimonOptionKey)
    {
        requireNonNull(paimonOptionKey, "paimonOptionKey is null");
        if (StringUtils.isNullOrWhitespaceOnly(paimonOptionKey)) {
            throw new IllegalArgumentException("paimonOptionKey is blank");
        }
        return EXCLUDED_TABLE_PROPERTY_OPTION_KEYS.contains(paimonOptionKey);
    }

    static boolean isRuntimeOnlyPaimonOptionKeyForWrite(String paimonOptionKey)
    {
        requireNonNull(paimonOptionKey, "paimonOptionKey is null");
        if (StringUtils.isNullOrWhitespaceOnly(paimonOptionKey)) {
            throw new IllegalArgumentException("paimonOptionKey is blank");
        }
        return isRuntimeOnlyPaimonOptionKey(paimonOptionKey)
                || WRITE_DYNAMIC_OPTION_ONLY_KEYS.contains(paimonOptionKey);
    }

    private static void validatePropertyKey(String propertyKey)
    {
        requireNonNull(propertyKey, "properties contains null option key");
        if (StringUtils.isNullOrWhitespaceOnly(propertyKey)) {
            throw new IllegalArgumentException("properties contains blank option key");
        }
    }

    public static List<OptionInfo> getOptionInfos()
    {
        return OPTION_INFOS;
    }

    public static Map<String, Object> tableProperties(Table table)
    {
        requireNonNull(table, "table is null");
        Map<String, String> options = table instanceof FileStoreTable fileStoreTable
                ? fileStoreTable.schema().options()
                : table.options();
        return tableProperties(options, table.primaryKeys(), table.partitionKeys());
    }

    static Map<String, Object> tableProperties(
            Map<String, String> options,
            List<String> primaryKeys,
            List<String> partitionKeys)
    {
        requireNonNull(options, "options is null");
        requireNonNull(primaryKeys, "primaryKeys is null");
        requireNonNull(partitionKeys, "partitionKeys is null");

        Map<String, Object> properties = new LinkedHashMap<>();
        if (!primaryKeys.isEmpty()) {
            properties.put(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.copyOf(primaryKeys));
        }
        if (!partitionKeys.isEmpty()) {
            properties.put(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.copyOf(partitionKeys));
        }

        for (OptionInfo optionInfo : OPTION_INFOS) {
            String optionValue = options.get(optionInfo.paimonOptionKey);
            if (optionValue != null) {
                properties.put(optionInfo.trinoOptionKey, optionValue);
            }
        }
        return Map.copyOf(properties);
    }

    private static List<OptionInfo> buildOptionInfos()
    {
        List<OptionInfo> optionInfos = new ArrayList<>();
        List<OptionWithMetaInfo> optionWithMetaInfos = extractConfigOptions(CoreOptions.class);
        for (OptionWithMetaInfo optionWithMetaInfo : optionWithMetaInfos) {
            if (shouldSkip(optionWithMetaInfo.option, optionWithMetaInfo.field)) {
                continue;
            }

            Optional<Class<?>> valueClass = optionValueClass(optionWithMetaInfo.field);
            String className = optionValueClassName(optionWithMetaInfo.field);
            optionInfos.add(new OptionInfo(
                    convertOptionKey(optionWithMetaInfo.option.key()),
                    optionWithMetaInfo.option.key(),
                    className,
                    shouldTrimOptionValue(optionWithMetaInfo.option.key(), valueClass)));
        }
        validateOptionInfos(optionInfos);
        return List.copyOf(optionInfos);
    }

    private static List<OptionInfo> buildAllOptionInfos()
    {
        List<OptionInfo> optionInfos = new ArrayList<>();
        List<OptionWithMetaInfo> optionWithMetaInfos = extractConfigOptions(CoreOptions.class);
        for (OptionWithMetaInfo optionWithMetaInfo : optionWithMetaInfos) {
            Optional<Class<?>> valueClass = optionValueClass(optionWithMetaInfo.field);
            String className = optionValueClassName(optionWithMetaInfo.field);
            optionInfos.add(new OptionInfo(
                    convertOptionKey(optionWithMetaInfo.option.key()),
                    optionWithMetaInfo.option.key(),
                    className,
                    shouldTrimOptionValue(optionWithMetaInfo.option.key(), valueClass)));
        }
        validateOptionInfos(optionInfos);
        return List.copyOf(optionInfos);
    }

    private static Map<String, OptionInfo> indexOptionInfosByTrinoKey(List<OptionInfo> optionInfos)
    {
        requireNonNull(optionInfos, "optionInfos is null");
        Map<String, OptionInfo> indexedOptionInfos = new LinkedHashMap<>();
        for (OptionInfo optionInfo : optionInfos) {
            indexedOptionInfos.put(optionInfo.trinoOptionKey, optionInfo);
        }
        return Map.copyOf(indexedOptionInfos);
    }

    private static Map<String, OptionInfo> indexOptionInfosByPaimonKey(List<OptionInfo> optionInfos)
    {
        requireNonNull(optionInfos, "optionInfos is null");
        Map<String, OptionInfo> indexedOptionInfos = new LinkedHashMap<>();
        for (OptionInfo optionInfo : optionInfos) {
            indexedOptionInfos.put(optionInfo.paimonOptionKey, optionInfo);
        }
        return Map.copyOf(indexedOptionInfos);
    }

    static void validateOptionInfos(List<OptionInfo> optionInfos)
    {
        requireNonNull(optionInfos, "optionInfos is null");
        Map<String, String> trinoToPaimonKeys = new LinkedHashMap<>();
        Map<String, String> paimonToTrinoKeys = new LinkedHashMap<>();
        for (OptionInfo optionInfo : optionInfos) {
            requireNonNull(optionInfo, "optionInfo is null");
            String trinoOptionKey = requireNonNull(optionInfo.trinoOptionKey, "trinoOptionKey is null");
            String paimonOptionKey = requireNonNull(optionInfo.paimonOptionKey, "paimonOptionKey is null");
            if (StringUtils.isNullOrWhitespaceOnly(trinoOptionKey)) {
                throw new IllegalArgumentException("trinoOptionKey is blank");
            }
            if (StringUtils.isNullOrWhitespaceOnly(paimonOptionKey)) {
                throw new IllegalArgumentException("paimonOptionKey is blank");
            }

            String previousPaimonOptionKey = trinoToPaimonKeys.putIfAbsent(trinoOptionKey, paimonOptionKey);
            if (previousPaimonOptionKey != null) {
                throw new IllegalStateException(
                        "Duplicate Trino table option key '%s' maps to Paimon keys '%s' and '%s'"
                                .formatted(trinoOptionKey, previousPaimonOptionKey, paimonOptionKey));
            }
            String previousTrinoOptionKey = paimonToTrinoKeys.putIfAbsent(paimonOptionKey, trinoOptionKey);
            if (previousTrinoOptionKey != null) {
                throw new IllegalStateException(
                        "Duplicate Paimon table option key '%s' maps to Trino keys '%s' and '%s'"
                                .formatted(paimonOptionKey, previousTrinoOptionKey, trinoOptionKey));
            }
        }
    }

    private static Optional<Class<?>> optionValueClass(Field field)
    {
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType parameterizedType) {
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            if (actualTypeArguments.length == 1) {
                Type actualTypeArgument = actualTypeArguments[0];
                if (actualTypeArgument instanceof Class<?> clazz) {
                    return Optional.of(clazz);
                }
                if (actualTypeArgument instanceof ParameterizedType actualParameterizedType
                        && actualParameterizedType.getRawType() instanceof Class<?> rawClass) {
                    return Optional.of(rawClass);
                }
            }
        }
        return Optional.empty();
    }

    private static String optionValueClassName(Field field)
    {
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType parameterizedType) {
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            if (actualTypeArguments.length == 1 && actualTypeArguments[0] instanceof Class<?> clazz) {
                return clazz.getSimpleName();
            }
        }
        return "";
    }

    private static boolean shouldTrimOptionValue(String paimonOptionKey, Optional<Class<?>> valueClass)
    {
        return valueClass
                .map(clazz -> shouldTrimOptionValue(paimonOptionKey, clazz))
                .orElse(false);
    }

    private static boolean shouldTrimOptionValue(String paimonOptionKey, Class<?> valueClass)
    {
        if (valueClass == String.class) {
            return isIdentifierLikeStringOption(paimonOptionKey);
        }
        return !Map.class.isAssignableFrom(valueClass)
                && !Collection.class.isAssignableFrom(valueClass);
    }

    private static boolean isIdentifierLikeStringOption(String paimonOptionKey)
    {
        return paimonOptionKey.equals(CoreOptions.FILE_FORMAT.key())
                || paimonOptionKey.equals(CoreOptions.CHANGELOG_FILE_FORMAT.key())
                || paimonOptionKey.equals(CoreOptions.MANIFEST_FORMAT.key())
                || paimonOptionKey.equals(CoreOptions.VECTOR_FILE_FORMAT.key())
                || paimonOptionKey.equals(CoreOptions.FILE_COMPRESSION.key())
                || paimonOptionKey.equals(CoreOptions.CHANGELOG_FILE_COMPRESSION.key())
                || paimonOptionKey.equals(CoreOptions.MANIFEST_COMPRESSION.key())
                || paimonOptionKey.equals(CoreOptions.SPILL_COMPRESSION.key())
                || paimonOptionKey.equals(CoreOptions.LOOKUP_CACHE_SPILL_COMPRESSION.key())
                || paimonOptionKey.equals(CoreOptions.FORMAT_TABLE_FILE_COMPRESSION.key())
                || paimonOptionKey.endsWith(".class")
                || paimonOptionKey.endsWith("-class")
                || paimonOptionKey.endsWith(".mode")
                || paimonOptionKey.endsWith("-mode")
                || paimonOptionKey.endsWith(".strategy")
                || paimonOptionKey.endsWith("-strategy")
                || paimonOptionKey.endsWith(".action")
                || paimonOptionKey.endsWith("-action")
                || paimonOptionKey.endsWith(".stats-mode")
                || paimonOptionKey.endsWith("-stats-mode");
    }

    private static boolean shouldSkip(ConfigOption<?> option, Field field)
    {
        requireNonNull(option, "option is null");
        requireNonNull(field, "field is null");
        if (isExcludedFromDocumentation(field)) {
            return true;
        }
        switch (field.getName()) {
            case "PRIMARY_KEY", "PARTITION", "PATH", "FILE_COMPRESSION_PER_LEVEL", "STREAMING_COMPACT" -> {
                return true;
            }
            default -> {
                return EXCLUDED_TABLE_PROPERTY_OPTION_KEYS.contains(option.key());
            }
        }
    }

    private static Set<String> excludedFromDocumentationOptionKeys(Class<?> clazz)
    {
        return extractConfigOptions(clazz).stream()
                .filter(optionWithMetaInfo -> isExcludedFromDocumentation(optionWithMetaInfo.field))
                .map(optionWithMetaInfo -> optionWithMetaInfo.option.key())
                .collect(toUnmodifiableSet());
    }

    private static boolean isExcludedFromDocumentation(Field field)
    {
        requireNonNull(field, "field is null");
        return field.getAnnotation(ExcludeFromDocumentation.class) != null;
    }

    public static String convertOptionKey(String key)
    {
        requireNonNull(key, "key is null");
        if (StringUtils.isNullOrWhitespaceOnly(key)) {
            throw new IllegalArgumentException("key is blank");
        }
        Matcher camelCaseMatcher = CAMEL_CASE_BOUNDARY.matcher(key);
        String snakeCaseKey = camelCaseMatcher.replaceAll("$1_$2");
        Matcher separatorMatcher = OPTION_KEY_SEPARATOR.matcher(snakeCaseKey.toLowerCase(Locale.ENGLISH));
        return separatorMatcher.replaceAll("_");
    }

    private static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz)
    {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field)) {
                    configOptions.add(new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
                }
            }
            return configOptions;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to extract config options from class " + clazz + '.', e);
        }
    }

    private static boolean isConfigOption(Field field)
    {
        return field.getType().equals(ConfigOption.class);
    }

    record OptionWithMetaInfo(ConfigOption<?> option, Field field) {}

    static class OptionInfo<T>
    {
        final String trinoOptionKey;
        final String paimonOptionKey;
        final String type;
        final boolean valueRequiresTrim;

        public OptionInfo(String trinoOptionKey, String paimonOptionKey, String type)
        {
            this(trinoOptionKey, paimonOptionKey, type, false);
        }

        public OptionInfo(String trinoOptionKey, String paimonOptionKey, String type, boolean valueRequiresTrim)
        {
            this.trinoOptionKey = trinoOptionKey;
            this.paimonOptionKey = paimonOptionKey;
            this.type = type;
            this.valueRequiresTrim = valueRequiresTrim;
        }
    }
}
