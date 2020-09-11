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
package io.prestosql.plugin.bigquery;

import java.util.Optional;

class ReadSessionCreatorConfig
{
    final boolean viewsEnabled;
    final Optional<String> viewMaterializationProject;
    final Optional<String> viewMaterializationDataset;
    final int viewExpirationTimeInHours;
    final int maxReadRowsRetries;

    ReadSessionCreatorConfig(
            boolean viewsEnabled,
            Optional<String> viewMaterializationProject,
            Optional<String> viewMaterializationDataset,
            int viewExpirationTimeInHours,
            int maxReadRowsRetries)
    {
        this.viewsEnabled = viewsEnabled;
        this.viewMaterializationProject = viewMaterializationProject;
        this.viewMaterializationDataset = viewMaterializationDataset;
        this.viewExpirationTimeInHours = viewExpirationTimeInHours;
        this.maxReadRowsRetries = maxReadRowsRetries;
    }
}
