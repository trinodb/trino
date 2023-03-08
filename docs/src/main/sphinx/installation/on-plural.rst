==============================
Deploying Trino on Kubernetes
==============================

`Plural <https://plural.sh?utm_medium=community&utm_source=trino>`_ is a free, open-source tool that enables you to self-host and manage Trino on Kubernetes with the cloud provider of your choice. It includes baked-in SSO, automated upgrades, secret encryption, and tailored Grafana dashboards.

Trino is available as a one-line install with the Plural CLI without prior Kubernetes knowledge.

Getting started
---------------

First, create an account on `app.plural.sh <https://app.plural.sh?utm_medium=community&utm_source=trino>`_. This is only to track your installations and allow for the delivery of automated upgrades. You will not be asked to provide any infrastructure credentials or sensitive information.

Next, install the Plural CLI by following steps 1-3 of `these instructions <https://docs.plural.sh/getting-started?utm_medium=community&utm_source=trino>`_.

You'll need a Git repository to store your Plural configuration. This will contain the Helm charts, Terraform config, and Kubernetes manifests that Plural will autogenerate for you.

You have two options:

* Run ``plural init`` in any directory to let Plural initiate an OAuth workflow to create a Git repo for you.
* Create a Git repo manually, clone it down, and run ``plural init`` inside it.

Running ``plural init`` will start a configuration wizard to configure your Git repo and cloud provider for use with Plural. You're now ready to install Trino on your Plural repo.

Installing Trino
----------------

To find the console bundle name for your cloud provider, run:

.. code-block:: text

    plural bundle list trino


Now, to add it your workspace, run the install command. If you're on AWS, this is what the command would look like:

.. code-block:: text

    plural bundle install trino trino-aws

Plural's Trino distribution has support for AWS, GCP, and Azure, so feel free to pick whichever best fits your infrastructure.

The CLI will prompt you to choose whether you want to use Plural OIDC. `OIDC <https://openid.net/connect/>`_ allows you to login to the applications you host on Plural with your login to `app.plural.sh <https://app.plural.sh?utm_medium=community&utm_source=trino>`_, acting as an SSO provider.

To generate the configuration and deploy your infrastructure, run:

.. code-block:: text

    plural build
    plural deploy --commit "deploying trino"

.. note::

    Deploys will generally take 10-20 minutes, based on your cloud provider.

Accessing your Trino installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, head over to ``trino.YOUR_SUBDOMAIN.onplural.sh`` to access the Trino UI. If you set up a different subdomain for Trino during installation, make sure to use that instead.

.. _installing-plural-console:

Installing the Plural Console
-----------------------------

To make management of your installation as simple as possible, we recommend installing the Plural Console. The console provides tools to manage resource scaling, receiving automated upgrades, creating dashboards tailored to your Trino installation, and log aggregation. This can be done using the exact same process as above, using AWS as an example:

.. code-block:: text

    plural bundle install console console-aws
    plural build
    plural deploy --commit "deploying the console too"

Accessing your Plural Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To monitor and manage your Trino installation, head over to the Plural Console at ``console.YOUR_SUBDOMAIN.onplural.sh``.

.. _uninstalling-trino:

Uninstalling Trino on Plural
----------------------------

To bring down your Plural installation of Trino at any time, run:

.. code-block:: text

    plural destroy trino

To bring your entire Plural deployment down, run:

.. code-block:: text

    plural destroy

.. note::

    Only do this if you're absolutely sure you want to bring down all associated resources with this repository.

Troubleshooting
---------------

If you have any issues with installing Trino on Plural, feel free to join our `Discord Community <https://discord.gg/pluralsh>`_ and we can help you out.

If you'd like to request any new features for our Trino installation, feel free to open an issue or PR in `this repo <https://github.com/pluralsh/plural-artifacts>`_.

Further reading
---------------

To learn more about what you can do with Plural and more advanced uses of the platform, feel free to dive deeper into our docs `here <https://docs.plural.sh?utm_medium=community&utm_source=trino>`_.
