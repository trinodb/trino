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
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * options utils.
 */
public class PaimonTableOptionUtils
{
    private PaimonTableOptionUtils() {}

    public static void buildOptions(Schema.Builder builder, Map<String, Object> properties)
    {
        List<OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();
        for (OptionInfo optionInfo : optionInfos) {
            if (properties.get(optionInfo.trinoOptionKey) != null
                    && !StringUtils.isNullOrWhitespaceOnly(
                    String.valueOf(properties.get(optionInfo.trinoOptionKey)))) {
                builder.option(
                        optionInfo.paimonOptionKey,
                        String.valueOf(properties.get(optionInfo.trinoOptionKey)));
            }
        }
    }

    public static List<OptionInfo> getOptionInfos()
    {
        List<OptionInfo> optionInfos = new ArrayList<>();
        List<OptionWithMetaInfo> optionWithMetaInfos = extractConfigOptions(CoreOptions.class);
        String className = "";
        for (OptionWithMetaInfo optionWithMetaInfo : optionWithMetaInfos) {
            if (shouldSkip(optionWithMetaInfo.field.getName())) {
                continue;
            }

            Type genericType = optionWithMetaInfo.field.getGenericType();
            if (genericType instanceof ParameterizedType parameterizedType) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                for (Type actualTypeArgument : actualTypeArguments) {
                    if (actualTypeArgument instanceof Class<?>) {
                        className = ((Class<?>) actualTypeArgument).getSimpleName();
                    }
                }
            }

            optionInfos.add(
                    new OptionInfo(
                            convertOptionKey(optionWithMetaInfo.option.key()),
                            optionWithMetaInfo.option.key(),
                            buildClass(className),
                            isEnum(className),
                            className));
        }
        return optionInfos;
    }

    private static boolean shouldSkip(String fieldName)
    {
        switch (fieldName) {
            case "PRIMARY_KEY":
            case "PARTITION":
            case "FILE_COMPRESSION_PER_LEVEL":
            case "STREAMING_COMPACT":
                return true;
            default:
                return false;
        }
    }

    private static boolean isEnum(String className)
    {
        switch (className) {
            case "StartupMode":
            case "MergeEngine":
            case "ChangelogProducer":
            case "LogConsistency":
            case "LogChangelogMode":
            case "StreamingReadMode":
                return true;
            default:
                return false;
        }
    }

    private static Class<?> buildClass(String className)
    {
        switch (className) {
            case "MergeEngine":
                return CoreOptions.MergeEngine.class;
            case "ChangelogProducer":
                return CoreOptions.ChangelogProducer.class;
            case "StartupMode":
                return CoreOptions.StartupMode.class;
            case "LogConsistency":
                return CoreOptions.LogConsistency.class;
            case "LogChangelogMode":
                return CoreOptions.LogChangelogMode.class;
            case "StreamingReadMode":
                return CoreOptions.StreamingReadMode.class;
            default:
                return null;
        }
    }

    private static String convertOptionKey(String key)
    {
        String regex = "[.\\-]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(key);
        return matcher.replaceAll("_");
    }

    private static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz)
    {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field)) {
                    configOptions.add(
                            new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
                }
            }
            return configOptions;
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract config options from class " + clazz + '.', e);
        }
    }

    private static boolean isConfigOption(Field field)
    {
        return field.getType().equals(ConfigOption.class);
    }

    static class OptionWithMetaInfo
    {
        final ConfigOption<?> option;
        final Field field;

        public OptionWithMetaInfo(ConfigOption<?> option, Field field)
        {
            this.option = option;
            this.field = field;
        }
    }

    static class OptionInfo<T>
    {
        String trinoOptionKey;
        String paimonOptionKey;
        Class<T> clazz;
        boolean isEnum;
        String type;

        public OptionInfo(
                String trinoOptionKey,
                String paimonOptionKey,
                Class<T> clazz,
                boolean isEnum,
                String type)
        {
            this.trinoOptionKey = trinoOptionKey;
            this.paimonOptionKey = paimonOptionKey;
            this.clazz = clazz;
            this.isEnum = isEnum;
            this.type = type;
        }
    }
}
