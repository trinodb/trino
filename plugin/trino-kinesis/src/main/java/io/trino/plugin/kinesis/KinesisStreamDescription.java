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
package io.trino.plugin.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * This Class maintains all the details of Kinesis stream like name, fields of data, Trino table stream is mapping to, tables's schema name
 */
public record KinesisStreamDescription(
        String tableName,
        String schemaName,
        String streamName,
        KinesisStreamFieldGroup message)
{
    public KinesisStreamDescription
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        requireNonNull(streamName, "streamName is null");
    }
}
