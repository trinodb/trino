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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.connector.CatalogHandle.CatalogHandleType.INFORMATION_SCHEMA;
import static io.trino.connector.CatalogHandle.CatalogHandleType.NORMAL;
import static io.trino.connector.CatalogHandle.CatalogHandleType.SYSTEM;
import static java.util.Objects.requireNonNull;

public final class CatalogHandle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(CatalogHandle.class).instanceSize();

    private static final String INFORMATION_SCHEMA_CONNECTOR_PREFIX = "$info_schema@";
    private static final String SYSTEM_TABLES_CONNECTOR_PREFIX = "$system@";

    private final String id;
    private final CatalogHandleType type;
    private final CatalogHandle rootCatalogHandle;

    public static CatalogHandle createRootCatalogHandle(String catalogName)
    {
        return new CatalogHandle(NORMAL, catalogName);
    }

    public static CatalogHandle createInformationSchemaCatalogHandle(CatalogHandle catalogHandle)
    {
        return new CatalogHandle(INFORMATION_SCHEMA, catalogHandle.getCatalogName());
    }

    public static CatalogHandle createSystemTablesCatalogHandle(CatalogHandle catalogHandle)
    {
        return new CatalogHandle(SYSTEM, catalogHandle.getCatalogName());
    }

    @JsonCreator
    public static CatalogHandle fromId(String id)
    {
        if (id.startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX)) {
            return new CatalogHandle(SYSTEM, id.substring(SYSTEM_TABLES_CONNECTOR_PREFIX.length()));
        }
        if (id.startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX)) {
            return new CatalogHandle(INFORMATION_SCHEMA, id.substring(INFORMATION_SCHEMA_CONNECTOR_PREFIX.length()));
        }
        return new CatalogHandle(NORMAL, id);
    }

    private CatalogHandle(CatalogHandleType type, String catalogName)
    {
        this.type = requireNonNull(type, "type is null");
        requireNonNull(catalogName, "catalogName is null");
        checkArgument(!catalogName.isEmpty(), "catalogName is empty");
        checkArgument(!catalogName.startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX) && !catalogName.startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX), "Catalog name is an internal name: %s", catalogName);
        switch (type) {
            case NORMAL:
                id = catalogName;
                this.rootCatalogHandle = this;
                break;
            case INFORMATION_SCHEMA:
                id = INFORMATION_SCHEMA_CONNECTOR_PREFIX + catalogName;
                this.rootCatalogHandle = new CatalogHandle(NORMAL, catalogName);
                break;
            case SYSTEM:
                id = SYSTEM_TABLES_CONNECTOR_PREFIX + catalogName;
                this.rootCatalogHandle = new CatalogHandle(NORMAL, catalogName);
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Gets the unique identifier of this catalog.
     */
    @JsonValue
    public String getId()
    {
        return id;
    }

    /**
     * Gets the actual raw catalog name for this handle.
     * This method should only be used when there are no other ways to access the catalog name.
     */
    public String getCatalogName()
    {
        return rootCatalogHandle.id;
    }

    public CatalogHandleType getType()
    {
        return type;
    }

    public CatalogHandle getRootCatalogHandle()
    {
        return rootCatalogHandle;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogHandle that = (CatalogHandle) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(id) +
                (rootCatalogHandle == this ? 0 : rootCatalogHandle.getRetainedSizeInBytes());
    }

    public enum CatalogHandleType
    {
        NORMAL(false),
        INFORMATION_SCHEMA(true),
        SYSTEM(true);

        private final boolean internal;

        CatalogHandleType(boolean internal)
        {
            this.internal = internal;
        }

        public boolean isInternal()
        {
            return internal;
        }
    }
}
