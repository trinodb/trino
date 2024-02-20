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
package io.trino.plugin.kafka.schema.file;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class FileTableDescriptionSupplierConfig
{
    private Set<String> tableNames = ImmutableSet.of();
    private File tableDescriptionDir = new File("etc/kafka/");

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    @ConfigDescription("Set of tables known to this connector")
    public FileTableDescriptionSupplierConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    @ConfigDescription("Folder holding JSON description files for Kafka topics")
    public FileTableDescriptionSupplierConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }
}
