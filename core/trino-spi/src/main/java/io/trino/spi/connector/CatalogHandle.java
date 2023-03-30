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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.Experimental;

import java.util.Objects;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.connector.CatalogHandle.CatalogHandleType.INFORMATION_SCHEMA;
import static io.trino.spi.connector.CatalogHandle.CatalogHandleType.NORMAL;
import static io.trino.spi.connector.CatalogHandle.CatalogHandleType.SYSTEM;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

@Experimental(eta = "2023-02-01")
public final class CatalogHandle
{
    private static final int INSTANCE_SIZE = instanceSize(CatalogHandle.class);

    private final String catalogName;
    private final CatalogHandleType type;
    private final CatalogHandle rootCatalogHandle;
    private final CatalogVersion version;

    public static CatalogHandle createRootCatalogHandle(String catalogName, CatalogVersion version)
    {
        return new CatalogHandle(catalogName, NORMAL, version);
    }

    public static CatalogHandle createInformationSchemaCatalogHandle(CatalogHandle catalogHandle)
    {
        return new CatalogHandle(catalogHandle.getCatalogName(), INFORMATION_SCHEMA, catalogHandle.getVersion());
    }

    public static CatalogHandle createSystemTablesCatalogHandle(CatalogHandle catalogHandle)
    {
        return new CatalogHandle(catalogHandle.getCatalogName(), SYSTEM, catalogHandle.getVersion());
    }

    @JsonCreator
    public static CatalogHandle fromId(String id)
    {
        requireNonNull(id, "id is null");

        int versionSplit = id.lastIndexOf(':');
        if (versionSplit <= 0) {
            throw new IllegalArgumentException("invalid id " + id);
        }

        int typeSplit = id.lastIndexOf(':', versionSplit - 1);
        if (typeSplit <= 0) {
            throw new IllegalArgumentException("invalid id " + id);
        }

        String catalogName = id.substring(0, typeSplit);
        CatalogHandleType type = CatalogHandleType.valueOf(id.substring(typeSplit + 1, versionSplit).toUpperCase(ROOT));
        CatalogVersion version = new CatalogVersion(id.substring(versionSplit + 1));
        return new CatalogHandle(catalogName, type, version);
    }

    private CatalogHandle(String catalogName, CatalogHandleType type, CatalogVersion version)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.type = requireNonNull(type, "type is null");
        this.version = requireNonNull(version, "version is null");
        requireNonNull(catalogName, "catalogName is null");
        if (catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName is empty");
        }
        this.rootCatalogHandle = switch (type) {
            case NORMAL -> this;
            case INFORMATION_SCHEMA, SYSTEM -> new CatalogHandle(catalogName, NORMAL, version);
        };
    }

    /**
     * Gets the unique identifier of this catalog.
     */
    @JsonValue
    public String getId()
    {
        return catalogName + ":" + type.toString().toLowerCase(ROOT) + ":" + version;
    }

    /**
     * Gets the actual raw catalog name for this handle.
     * This method should only be used when there are no other ways to access the catalog name.
     */
    public String getCatalogName()
    {
        return catalogName;
    }

    public CatalogHandleType getType()
    {
        return type;
    }

    public CatalogVersion getVersion()
    {
        return version;
    }

    public CatalogHandle getRootCatalogHandle()
    {
        return rootCatalogHandle;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        return other instanceof CatalogHandle that &&
                Objects.equals(catalogName, that.catalogName) &&
                type == that.type &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, type, version);
    }

    @Override
    public String toString()
    {
        return catalogName;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(catalogName) +
                version.getRetainedSizeInBytes() +
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

    @Experimental(eta = "2023-02-01")
    public static final class CatalogVersion
            implements Comparable<CatalogVersion>
    {
        private static final int INSTANCE_SIZE = instanceSize(CatalogVersion.class);

        private final String version;

        /**
         * Version of a catalog.  The string maybe compared lexicographically using ASCII, and to determine which catalog version is newer.
         */
        @JsonCreator
        public CatalogVersion(String version)
        {
            requireNonNull(version, "version is null");
            if (version.isEmpty()) {
                throw new IllegalArgumentException("version is empty");
            }
            for (int i = 0; i < version.length(); i++) {
                if (!isAllowedCharacter(version.charAt(i))) {
                    throw new IllegalArgumentException("invalid version: " + version);
                }
            }

            this.version = version;
        }

        private static boolean isAllowedCharacter(char c)
        {
            return ('0' <= c && c <= '9') ||
                    ('a' <= c && c <= 'z') ||
                    c == '_' ||
                    c == '-';
        }

        @Override
        public int compareTo(CatalogVersion other)
        {
            return version.compareTo(other.version);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            return other instanceof CatalogVersion that &&
                    version.equals(that.version);
        }

        @Override
        public int hashCode()
        {
            return version.hashCode();
        }

        @JsonValue
        @Override
        public String toString()
        {
            return version;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(version);
        }
    }
}
