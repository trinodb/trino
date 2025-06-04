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
package io.trino.plugin.hive;

import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.lang.String.format;

public class PartitionNotFoundException
        extends NotFoundException
{
    public PartitionNotFoundException(SchemaTableName tableName, List<String> partitionValue)
    {
        super(format("Partition '%s' not found", tableName), null);
    }
}
