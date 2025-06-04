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

package io.trino.spi;

/**
 * Signifies whether a Materialized View refresh operation could be done incrementally or only fully.
 * The engine will make this determination during analysis/planning, but the connector may take the ultimate decision,
 * based on its level of incremental refresh support.
 */
public enum RefreshType {
    INCREMENTAL,
    FULL
}
