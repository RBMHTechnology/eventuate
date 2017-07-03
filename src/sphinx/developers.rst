.. _developers:

----------
Developers
----------

Contributing to Eventuate
-------------------------

If you haven't already done, please sign the `Individual Contributor License Agreement`_ (CLA). To sign it online, send an email to `opensource@redbullmediahouse.com`_ including your full name and email address. You'll receive an email with a link for filling and signing the CLA.

Then, fork the `Eventuate Github repository`_ and start working on a feature branch. When you are done, create a pull request towards the targeted branch. When there’s consensus on the review, someone from the Eventuate team will merge it.

Please follow the best practices mentioned in `Git workflow`_ and the guidelines for `Git commit messages`_. If you need help or want to discuss your contribution, post a message to the `Eventuate mailing list`_.

Building Eventuate
------------------

Eventuate builds require an installation of the sbt_ build tool and a local clone of the `Eventuate Github repository`_ or your personal fork.

Binaries
~~~~~~~~

After having cloned the repository, change to the ``eventuate`` directory::

    cd eventuate

and run the tests with::

    sbt test

which runs unit, integration and multi-jvm tests. You can run them separately with::

   sbt test:testOnly
   sbt it:testOnly
   sbt multi-jvm:test

To publish the binaries to the local Ivy repository, run::

    sbt publishLocal

To publish the binaries to the local Maven repository, run::

    sbt publishM2

Documentation
~~~~~~~~~~~~~

Our documentation is written in reStructuredText_ and located in ``src/sphinx``, except for the User Guide, which is located in `examples/user-guide`.
Building the documentation requires an installation of Python_ and Sphinx_.

The following installation instructions are for Mac OS X. If Python isn’t already installed, install Homebrew_ first and then Python with::

    brew install python

Next, install pip::

    sudo easy_install pip

and then Sphinx with::

    sudo pip install sphinx

Additionally, the Sphinx `Read the Docs`_ scheme is required::

    sudo pip install sphinx_rtd_scheme

Now build the documentation with::

    sbt makeSite

and open ``target/site/index.html`` in your browser.

.. hint:
   If you get an ``unknown locale`` error during the build, define the following environment variables::

       export LANG=en_US.UTF-8
       export LC_ALL=en_US.UTF-8

.. _sbt: http://www.scala-sbt.org/
.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Read the Docs: https://readthedocs.org/
.. _Sphinx: http://sphinx-doc.org/
.. _Python: https://www.python.org/
.. _Homebrew: http://brew.sh/

.. _Eventuate Github repository: https://github.com/RBMHTechnology/eventuate
.. _Eventuate mailing list: https://groups.google.com/forum/#!forum/eventuate
.. _Individual Contributor License Agreement: http://rbmhtechnology.github.io/cla/cla.pdf
.. _opensource@redbullmediahouse.com: mailto:opensource@redbullmediahouse.com?subject=Individual%20CLA

.. _Git workflow: https://sandofsky.com/blog/git-workflow.html
.. _Git commit messages: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
