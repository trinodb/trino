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

import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class ConnectorTableVersion
{
    private final PointerType pointerType;
    private final Type versionType;
    private final Object version;

    public ConnectorTableVersion(PointerType pointerType, Type versionType, Object version)
    {
        requireNonNull(pointerType, "pointerType is null");
        requireNonNull(versionType, "versionType is null");
        requireNonNull(version, "version is null");
        this.pointerType = pointerType;
        this.versionType = versionType;
        this.version = version;
    }

    public PointerType getPointerType()
    {
        return pointerType;
    }

    public Type getVersionType()
    {
        return versionType;
    }

    public Object getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return new StringBuilder("ConnectorTableVersion{")
                .append("pointerType=").append(pointerType)
                .append(", versionType=").append(versionType)
                .append(", version=").append(version)
                .append('}')
                .toString();
    }
}
