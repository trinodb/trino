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
package io.trino.spi.security;

/**
 * Identifies why the engine evaluates part of a query using an effective identity
 * that differs from the identity of the query. Passed to
 * {@link SystemAccessControl#checkCanSetEffectiveIdentity} so policies can distinguish
 * the engine-controlled identity switches below from explicit user impersonation, which
 * is handled by {@link SystemAccessControl#checkCanImpersonateUser}.
 */
public enum IdentitySwitchReason
{
    VIEW_OWNER,
    MATERIALIZED_VIEW_OWNER,
    FUNCTION_OWNER,
    ROW_FILTER,
    COLUMN_MASK,
}
