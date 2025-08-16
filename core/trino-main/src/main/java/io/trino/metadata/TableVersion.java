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
package io.trino.metadata;

import io.trino.spi.connector.PointerType;
import io.trino.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record TableVersion(PointerType pointerType, Type objectType, Object pointer)
{
    public TableVersion
    {
        requireNonNull(objectType, "objectType is null");
    }

    public static TableVersion toTableVersion(String branchName)
    {
        return new TableVersion(TARGET_ID, VARCHAR, utf8Slice(branchName));
    }
}
