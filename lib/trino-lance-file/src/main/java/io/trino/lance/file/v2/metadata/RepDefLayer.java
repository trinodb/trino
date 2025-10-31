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
package io.trino.lance.file.v2.metadata;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public enum RepDefLayer
{
    ALL_VALID_ITEM,
    ALL_VALID_LIST,
    NULLABLE_ITEM,
    NULLABLE_LIST,
    EMPTYABLE_LIST,
    NULLABLE_AND_EMPTYABLE_LIST;

    public static RepDefLayer fromProto(build.buf.gen.lance.encodings21.RepDefLayer proto)
    {
        return switch (proto) {
            case REPDEF_ALL_VALID_ITEM -> ALL_VALID_ITEM;
            case REPDEF_ALL_VALID_LIST -> ALL_VALID_LIST;
            case REPDEF_NULLABLE_ITEM -> NULLABLE_ITEM;
            case REPDEF_NULLABLE_LIST -> NULLABLE_LIST;
            case REPDEF_EMPTYABLE_LIST -> EMPTYABLE_LIST;
            case REPDEF_NULL_AND_EMPTY_LIST -> NULLABLE_AND_EMPTYABLE_LIST;
            default -> throw new IllegalArgumentException("Unknown RepDefLayer: " + proto);
        };
    }

    public static List<RepDefLayer> fromProtoList(List<build.buf.gen.lance.encodings21.RepDefLayer> protos)
    {
        return protos.stream().map(RepDefLayer::fromProto).collect(toImmutableList());
    }

    public boolean isList()
    {
        return switch (this) {
            case ALL_VALID_LIST, NULLABLE_LIST, EMPTYABLE_LIST, NULLABLE_AND_EMPTYABLE_LIST -> true;
            default -> false;
        };
    }

    public boolean isAllValid()
    {
        return switch (this) {
            case ALL_VALID_ITEM, ALL_VALID_LIST, EMPTYABLE_LIST -> true;
            default -> false;
        };
    }

    public int numDefLevels()
    {
        return switch (this) {
            case ALL_VALID_ITEM, ALL_VALID_LIST -> 0;
            case NULLABLE_ITEM, NULLABLE_LIST, EMPTYABLE_LIST -> 1;
            case NULLABLE_AND_EMPTYABLE_LIST -> 2;
        };
    }
}
