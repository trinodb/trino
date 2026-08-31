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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.SessionRepresentation;
import io.trino.connector.CatalogHandle;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record CallProcedureRequest(
        CatalogHandle catalogHandle,
        CatalogProperties catalogProperties,
        SchemaTableName procedureName,
        List<NullableValue> argumentValues,
        SessionRepresentation session)
{
    public CallProcedureRequest
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(catalogProperties, "catalogProperties is null");
        requireNonNull(procedureName, "procedureName is null");
        argumentValues = ImmutableList.copyOf(argumentValues);
        requireNonNull(session, "session is null");
    }
}
