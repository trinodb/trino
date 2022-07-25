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
 * ConnectorTableExecuteHandle identifies instance of executing a connector provided table procedure on a specific table.
 *
 * ConnectorTableExecuteHandle for planning is obtained by call to {@link ConnectorMetadata#getTableHandleForExecute} for given
 * procedure name and table.
 *
 * Then after planning, just before execution start, ConnectorTableExecuteHandle is refreshed via call to
 * {@link ConnectorMetadata#beginTableExecute(ConnectorSession, ConnectorTableExecuteHandle, ConnectorTableHandle)}
 * The tableHandle passed to beginTableExecute is one obtained from matching TableScanNode in the plan.
 */
public interface ConnectorTableExecuteHandle {}
