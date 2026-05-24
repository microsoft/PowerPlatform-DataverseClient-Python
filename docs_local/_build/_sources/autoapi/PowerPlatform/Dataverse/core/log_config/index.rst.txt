PowerPlatform.Dataverse.core.log_config
=======================================

.. py:module:: PowerPlatform.Dataverse.core.log_config

.. autoapi-nested-parse::

   Local file logging configuration for Dataverse SDK HTTP diagnostics.

   Provides :class:`~PowerPlatform.Dataverse.core.log_config.LogConfig`, an opt-in configuration for writing request/response
   traces to ``.log`` files with automatic header redaction and timestamped filenames.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.core.log_config.LogConfig


Module Contents
---------------

.. py:class:: LogConfig

   Configuration for local HTTP diagnostics logging.

   When provided to :class:`~PowerPlatform.Dataverse.client.DataverseClient` via
   :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig`, every HTTP request
   and response is logged to timestamped ``.log`` files in the specified folder.
   Sensitive headers (e.g. ``Authorization``) are automatically redacted.

   :param log_folder: Directory path for log files. Created automatically if missing.
       Default: ``"./dataverse_logs"``
   :param log_file_prefix: Filename prefix. Timestamp is appended automatically.
       Default: ``"dataverse"``  →  ``dataverse_20260310_143022.log``
   :param max_body_bytes: Maximum bytes of request/response body to capture.
       ``0`` (default) disables body capture. Enable only for active debugging
       sessions — bodies may contain PII and sensitive business data.
   :param redacted_headers: Header names (case-insensitive) whose values are
       replaced with ``"[REDACTED]"`` in logs. Defaults include
       ``Authorization``, ``Proxy-Authorization``, etc.
   :param log_level: Python logging level name. Default: ``"DEBUG"``.
   :param max_file_bytes: Max size per log file before rotation (bytes).
       Default: ``10_485_760`` (10 MB).
   :param backup_count: Number of rotated backup files to keep. Default: ``5``.


   .. py:attribute:: log_folder
      :type:  str
      :value: './dataverse_logs'



   .. py:attribute:: log_file_prefix
      :type:  str
      :value: 'dataverse'



   .. py:attribute:: max_body_bytes
      :type:  int
      :value: 0



   .. py:attribute:: redacted_headers
      :type:  FrozenSet[str]


   .. py:attribute:: log_level
      :type:  str
      :value: 'DEBUG'



   .. py:attribute:: max_file_bytes
      :type:  int
      :value: 10485760



   .. py:attribute:: backup_count
      :type:  int
      :value: 5



