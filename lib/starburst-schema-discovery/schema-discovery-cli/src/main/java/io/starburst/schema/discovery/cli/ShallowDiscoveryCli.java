/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.cli;

import io.starburst.schema.discovery.cli.commands.ShallowDiscoveryCommand;
import picocli.CommandLine;

public class ShallowDiscoveryCli
{
    private ShallowDiscoveryCli() {}

    public static void main(String[] args)
    {
        CommandLine commandLine = new CommandLine(new ShallowDiscoveryCommand());
        commandLine.setExecutionExceptionHandler((e, __, ___) -> {
            if (Boolean.getBoolean("DEBUG")) {
                e.printStackTrace();
            }
            else {
                System.err.println(e.getMessage());
            }
            return -1;
        });
        commandLine.execute(args);
    }
}
