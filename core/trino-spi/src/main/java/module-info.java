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
module trino.spi {
    requires com.fasterxml.jackson.annotation;
    requires com.google.errorprone.annotations;
    requires transitive io.opentelemetry.api;
    requires jakarta.annotation;
    requires transitive slice;

    exports io.trino.spi;
    exports io.trino.spi.block;
    exports io.trino.spi.catalog;
    exports io.trino.spi.classloader;
    exports io.trino.spi.connector;
    exports io.trino.spi.eventlistener;
    exports io.trino.spi.exchange;
    exports io.trino.spi.expression;
    exports io.trino.spi.function;
    exports io.trino.spi.function.table;
    exports io.trino.spi.memory;
    exports io.trino.spi.metrics;
    exports io.trino.spi.predicate;
    exports io.trino.spi.resourcegroups;
    exports io.trino.spi.security;
    exports io.trino.spi.session;
    exports io.trino.spi.spool;
    exports io.trino.spi.statistics;
    exports io.trino.spi.transaction;
    exports io.trino.spi.type;
}
