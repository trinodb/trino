=========================
SET SESSION AUTHORIZATION
=========================

Synopsis
--------

.. code-block:: none

    SET SESSION AUTHORIZATION username

Description
-----------

Changes the current user of the session.
For the ``SET SESSION AUTHORIZATION username`` statement to succeed,
the current user must be able to impersonate the specified user.
User impersonation can be enabled in the file-based system access
control with :ref:`impersonation rules <impersonation_rules>`.

Examples
--------

In the following example, the original user when the connection to Presto is made is Kevin.
The following sets the session authorization user to John::

    SET SESSION AUTHORIZATION 'John';

Queries will now execute as John instead of Kevin.

All supported syntax to change the session authorization users are shown below.

Changing the session authorization with single quotes::

    SET SESSION AUTHORIZATION 'John';

Changing the session authorization with double quotes::

    SET SESSION AUTHORIZATION "John";

Changing the session authorization without quotes::

    SET SESSION AUTHORIZATION John;

