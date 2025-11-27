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

public sealed interface PageLayout
        permits MiniBlockLayout, AllNullLayout, FullZipLayout
{
    static PageLayout fromProto(build.buf.gen.lance.encodings21.PageLayout proto)
    {
        return switch (proto.getLayoutCase()) {
            case MINI_BLOCK_LAYOUT -> MiniBlockLayout.fromProto(proto.getMiniBlockLayout());
            case ALL_NULL_LAYOUT -> AllNullLayout.fromProto(proto.getAllNullLayout());
            case FULL_ZIP_LAYOUT -> FullZipLayout.fromProto(proto.getFullZipLayout());
            default -> throw new RuntimeException("Unknown layout: " + proto.getLayoutCase());
        };
    }
}
