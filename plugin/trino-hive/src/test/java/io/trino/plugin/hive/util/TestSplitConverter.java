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
package io.trino.plugin.hive.util;

import java.util.HashMap;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class TestSplitConverter
        implements CustomSplitConverter
{
    public static final String CUSTOM_SPLIT_CLASS_KEY = "custom_split_class";
    public static final String CUSTOM_FIELD = "custom_field";

    @Override
    public Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split)
    {
        if (split instanceof CustomSplit customSplit) {
            Map<String, String> customSplitInfo = new HashMap<>();
            customSplitInfo.put(CUSTOM_SPLIT_CLASS_KEY, CustomSplit.class.getName());
            customSplitInfo.put(CUSTOM_FIELD, String.valueOf(customSplit.getCustomField()));
            return Optional.of(customSplitInfo);
        }
        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Properties schema) throws IOException
    {
        String customSplitClass = schema.getProperty(CUSTOM_SPLIT_CLASS_KEY);
        if (CustomSplit.class.getName().equals(customSplitClass)) {
            return Optional.of(new CustomSplit(
                    split,
                    Integer.parseInt(schema.getProperty(CUSTOM_FIELD))));
        }
        return Optional.empty();
    }
}
