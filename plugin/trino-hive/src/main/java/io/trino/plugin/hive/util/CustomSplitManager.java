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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static java.util.Objects.requireNonNull;

public class CustomSplitManager
{
    public static final String IS_CUSTOM_SPLIT = "is_custom_split";

    private final Set<CustomSplitConverter> customSplitConverters;

    @Inject
    public CustomSplitManager(Set<CustomSplitConverter> customSplitConverters)
    {
        this.customSplitConverters = requireNonNull(customSplitConverters, "customSplitConverters is null");
    }

    public CustomSplitManager()
    {
        this(ImmutableSet.of());
    }

    public Map<String, String> extractCustomSplitInfo(FileSplit split)
    {
        for (CustomSplitConverter converter : customSplitConverters) {
            Optional<Map<String, String>> customSplitData = converter.extractCustomSplitInfo(split);
            if (customSplitData.isPresent()) {
                customSplitData.get().put(IS_CUSTOM_SPLIT, "true");
                return customSplitData.get();
            }
        }
        return ImmutableMap.of();
    }

    public FileSplit recreateSplitWithCustomInfo(FileSplit split, Properties schema)
    {
        for (CustomSplitConverter converter : customSplitConverters) {
            Optional<FileSplit> fileSplit;
            try {
                fileSplit = converter.recreateFileSplitWithCustomInfo(split, schema);
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT,
                        String.format("Split converter %s failed to create FileSplit.", converter.getClass()), e);
            }
            if (fileSplit.isPresent()) {
                return fileSplit.get();
            }
        }
        return split;
    }
}
