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

/**
 * This enum represents the type of pointer used in TableVersion.
 * Temporal pointers are used in TIMESTAMP based query periods.
 * Snapshot id pointers are used in VERSION based query periods.
 */
public enum PointerType
{
    TEMPORAL,
    TARGET_ID
}
