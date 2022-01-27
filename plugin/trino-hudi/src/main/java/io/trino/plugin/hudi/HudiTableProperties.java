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

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class HudiTableProperties
{
    public static final String BASE_FILE_FORMAT_PROPERTY = "format";
    public static final String LOCATION_PROPERTY = "location";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HudiTableProperties(HudiConfig hudiConfig)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        BASE_FILE_FORMAT_PROPERTY,
                        "File format for the table",
                        HoodieFileFormat.class,
                        hudiConfig.getBaseFileFormat(),
                        false))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static HoodieFileFormat getBaseFileFormat(Map<String, Object> tableProperties)
    {
        return (HoodieFileFormat) tableProperties.get(BASE_FILE_FORMAT_PROPERTY);
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }
}
