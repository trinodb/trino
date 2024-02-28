/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.tests.odbc;

public class DummyClassToPleaseMaven
{
    // This class is here so please maven. Starting with maven-install-plugin 3.1.1, when no module has no non-test code
    // maven complains
    //     The packaging plugin for this project did not assign a main file to the project but it has attachments. Change packaging to 'pom'
    // However, changing packaging to pom disables test execution.
}
