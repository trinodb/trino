/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.synapse.SynapseServer;
import io.trino.execution.FailureInjector;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.Assertions;
import org.testng.SkipException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.DEFAULT_CATALOG_NAME;
import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public abstract class BaseSynapseFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    public BaseSynapseFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
        synapseServer = new SynapseServer();
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        return createSynapseQueryRunner(
                configProperties,
                synapseServer,
                DEFAULT_CATALOG_NAME,
                Map.of(),
                coordinatorProperties,
                requiredTpchTables,
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of(
                            "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                });
    }

    @Override
    protected void testExplainAnalyze()
    {
        this.testSelect("EXPLAIN ANALYZE SELECT regionkey, count(*) FROM nation GROUP BY regionkey");
        this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation WITH NO DATA"), "EXPLAIN ANALYZE INSERT INTO <table> SELECT * FROM nation", Optional.of("DROP TABLE <table>"));
    }

    @Override
    protected void testAnalyzeTable()
    {
        Assertions.assertThatThrownBy(() -> {
            testNonSelect(Optional.empty(), Optional.of("CREATE TABLE <table> AS SELECT * FROM nation"), "ANALYZE <table>", Optional.of("DROP TABLE <table>"), false);
        }).hasMessageContaining("This connector does not support analyze");
        throw new SkipException("skipped");
    }

    @Override
    protected void testCreateTable()
    {
        this.testTableModification(Optional.empty(), "CREATE TABLE <table> AS SELECT * FROM nation", Optional.of("DROP TABLE <table>"));
    }

    @Override
    protected void testInsert()
    {
        this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation WITH NO DATA"), "INSERT INTO <table> SELECT * FROM nation", Optional.of("DROP TABLE <table>"));
    }

    @Override
    protected void testDelete()
    {
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM nation");
        String testQuery = "DELETE FROM <table> WHERE nationkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");
        this.assertThatQuery(testQuery).withSetupQuery(setupQuery).withCleanupQuery(cleanupQuery).isCoordinatorOnly();
    }

    @Override
    protected void testDeleteWithSubquery()
    {
        Assertions.assertThatThrownBy(() -> {
            this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation"), "DELETE FROM <table> WHERE nationkey IN (SELECT custkey FROM customer WHERE nationkey = 1)", Optional.of("DROP TABLE <table>"));
        }).hasMessageContaining("This connector does not support modifying table rows");
        throw new SkipException("skipped");
    }

    @Override
    protected void testRefreshMaterializedView()
    {
        Assertions.assertThatThrownBy(() -> {
            this.testTableModification(Optional.of("CREATE MATERIALIZED VIEW <table> AS SELECT * FROM nation"), "REFRESH MATERIALIZED VIEW <table>", Optional.of("DROP MATERIALIZED VIEW <table>"));
        }).hasMessageContaining("This connector does not support creating materialized views");
        throw new SkipException("skipped");
    }

    @Override
    protected void testUpdate()
    {
        Assertions.assertThatThrownBy(() -> {
            this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation"), "UPDATE <table> SET name = 'BRASIL' WHERE nationkey = 2", Optional.of("DROP TABLE <table>"));
        }).hasMessageContaining("This connector does not support modifying table rows");
        throw new SkipException("skipped");
    }

    @Override
    protected void testUpdateWithSubquery()
    {
        Assertions.assertThatThrownBy(() -> {
            this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation"), "UPDATE <table> SET name = 'Brasil' WHERE nationkey = (SELECT min(custkey) + 1 FROM customer)", Optional.of("DROP TABLE <table>"));
        }).hasMessageContaining("This connector does not support modifying table rows");
        throw new SkipException("skipped");
    }

    @Override
    protected void testMerge()
    {
        Assertions.assertThatThrownBy(() -> {
            this.testTableModification(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation"), "MERGE INTO <table> t\nUSING (SELECT nationkey, 'new_nation' name FROM <table>) s\nON t.nationkey = s.nationkey\nWHEN MATCHED AND s.nationkey > 10\n    THEN UPDATE SET name = t.name || s.name\nWHEN MATCHED AND s.nationkey <= 10\n    THEN DELETE\n", Optional.of("DROP TABLE <table>"));
        }).hasMessageContaining("This connector does not support modifying table rows");
        throw new SkipException("skipped");
    }

    @Override
    protected void testRequestTimeouts()
    {
        this.assertThatQuery("SELECT * FROM nation").experiencing(FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT).at(leafStage()).failsWithoutRetries((failure) -> {
            failure.hasMessageContaining("Encountered too many errors talking to a worker node");
        }).finishesSuccessfully();
        this.assertThatQuery("SELECT * FROM nation").experiencing(FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT).at(boundaryDistributedStage()).failsWithoutRetries((failure) -> {
            failure.hasMessageContaining("Encountered too many errors talking to a worker node");
        }).finishesSuccessfully();
        if (this.areWriteRetriesSupported()) {
            this.assertThatQuery("INSERT INTO <table> SELECT * FROM nation").withSetupQuery(Optional.of("CREATE TABLE <table> AS SELECT * FROM nation WITH NO DATA")).withCleanupQuery(Optional.of("DROP TABLE <table>")).experiencing(FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT).at(leafStage()).failsWithoutRetries((failure) -> {
                failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer");
            }).finishesSuccessfullyWithoutTaskFailures();
        }
    }
}
