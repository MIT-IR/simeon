The python API of this tool is divided into the following sections:

- download: Handles the downloading of data files from s3. It matches up with the ``simeon download`` and ``simeon split`` commands

- upload: Handles the uploading of data to GCS and BigQuery. It matches up with the ``simeon push`` command

- report: Handles the generation of secondary tables in BigQuery. It matches up with the ``simeon report`` command


Components of the :class:`~simeon.download` package
----------------------------------------------------
AWS module
^^^^^^^^^^^
.. automodule:: simeon.download.aws
   :members:

Email opt-in module
^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.download.emails
   :members:

Tracking logs module
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.download.logs
   :members:

SQL data files module
^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.download.sqls
   :members:

Utilities module for the download package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.download.utilities
   :members:


Components of the :class:`~simeon.upload` package
----------------------------------------------------
GCP module
^^^^^^^^^^^^
.. automodule:: simeon.upload.gcp
   :members:

Utilities module for the upload package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.upload.utilities
   :members:


Components of the :class:`~simeon.report` package
-----------------------------------------------------
Utilities module for the report package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.report.utilities
   :members:

Components of the :class:`~simeon.exceptions` package
-------------------------------------------------------
Exceptions module
^^^^^^^^^^^^^^^^^^
.. automodule:: simeon.exceptions
   :members: