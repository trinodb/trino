=================
Graceful Shutdown
=================

Presto has a graceful shutdown API that can be used exclusively on workers in
order to ensure that they terminate without affecting running queries, given a sufficient
grace period.

You can invoke the API like this:

.. code-block:: bash

    curl -v -X PUT -d '"SHUTTING_DOWN"' -H "Content-type: application/json" \
        http://worker:8081/v1/info/state

You should keep in mind:

* If your cluster is secure, you will need to provide a basic-authorization header, or
  satisfy whatever other security you have enabled.
* If you have HTTPS/TLS enabled, you will have to ensure the worker certificate is
  CA signed, or trusted by the server calling the shut down endpoint.  Otherwise,
  you an make the call ``--insecure``, but that isn't recommended.

Shutdown Behavior
-----------------

Once the API is called, the worker will:

* Have its state set to to ``SHUTTING_DOWN``.
* Sleep for the configured grace period (``shutdown.grace-period``) which defaults to 2 minutes.
    * After this the coordinator should notice the pending shut-down and stop sending tasks to the worker.
* Block until all active tasks are complete.
* Sleep for the grace period again in order to ensure the coordinator sees all tasks are complete.
* Shut down the server/JVM.
