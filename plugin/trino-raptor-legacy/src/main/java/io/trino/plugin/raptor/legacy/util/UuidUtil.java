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
package io.trino.plugin.raptor.legacy.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.internal.strategies.LoggableBinderArgument;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

public final class UuidUtil
{
    private UuidUtil() {}

    public static final class UuidArgumentFactory
            extends AbstractArgumentFactory<UUID>
    {
        public UuidArgumentFactory()
        {
            super(Types.VARBINARY);
        }

        @Override
        protected Argument build(UUID uuid, ConfigRegistry config)
        {
            return new LoggableBinderArgument<>(uuid, (statement, index, value) ->
                    statement.setBytes(index, uuidToBytes(value)));
        }
    }

    public static final class UuidColumnMapper
            implements ColumnMapper<UUID>
    {
        @Override
        public UUID map(ResultSet r, int index, StatementContext ctx)
                throws SQLException
        {
            return uuidFromBytes(r.getBytes(index));
        }
    }

    public static UUID uuidFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    public static byte[] uuidToBytes(UUID uuid)
    {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * @param uuidSlice textual representation of UUID
     * @return byte representation of UUID
     * @throws IllegalArgumentException if uuidSlice is not a valid string representation of UUID
     */
    public static Slice uuidStringToBytes(Slice uuidSlice)
    {
        UUID uuid = UUID.fromString(uuidSlice.toStringUtf8());
        byte[] bytes = uuidToBytes(uuid);
        return Slices.wrappedBuffer(bytes);
    }
}
