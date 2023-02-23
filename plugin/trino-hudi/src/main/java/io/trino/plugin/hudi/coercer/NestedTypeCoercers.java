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
package io.trino.plugin.hudi.coercer;

import io.trino.plugin.hive.HivePageSource;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.block.Block;
import io.trino.spi.type.TypeManager;

import java.util.function.Function;

public class NestedTypeCoercers
{
    private NestedTypeCoercers() {}

    public static Function<Block, Block> createListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        return new HivePageSource.ListCoercer(typeManager, fromHiveType, toHiveType);
    }

    public static Function<Block, Block> createMapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        return new HivePageSource.MapCoercer(typeManager, fromHiveType, toHiveType);
    }

    public static Function<Block, Block> createStructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        return new HivePageSource.StructCoercer(typeManager, fromHiveType, toHiveType);
    }
}
