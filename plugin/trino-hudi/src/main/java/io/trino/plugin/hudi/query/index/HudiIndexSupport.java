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
package io.trino.plugin.hudi.query.index;

import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.List;
import java.util.Map;

public interface HudiIndexSupport
{
    Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(
            HoodieTableMetadata metadataTable,
            Map<String, List<FileSlice>> inputFileSlices,
            TupleDomain<String> regularColumnPredicates);

    boolean canApply(TupleDomain<String> tupleDomain);

    default boolean shouldKeepFileSlice(FileSlice slice) {return true;}
}
