PowerPlatform.Dataverse.core.config
===================================

.. py:module:: PowerPlatform.Dataverse.core.config

.. autoapi-nested-parse::

   Dataverse client configuration.

   Provides :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig`, a lightweight
   immutable container for locale and (reserved) HTTP tuning options plus the
   convenience constructor :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.core.config.OperationContext
   PowerPlatform.Dataverse.core.config.DataverseConfig


Module Contents
---------------

.. py:class:: OperationContext

   Caller-defined context appended to outbound ``User-Agent`` headers.

   The context string is validated to be semicolon-separated ``key=value`` pairs
   using only allowed keys (``app``, ``skill``, ``agent``) with values from
   closed allowlists.  Free-form text, email addresses, PII, and unknown keys
   are rejected.

   :param user_agent_context: Attribution string in ``key=value;key=value`` format.
   :type user_agent_context: :class:`str`

   :raises ValueError: If the string is empty, contains control characters,
       does not match the required ``key=value`` format, or uses unknown
       keys/values.


   .. py:attribute:: user_agent_context
      :type:  str


.. py:class:: DataverseConfig

   Configuration settings for Dataverse client operations.

   :param language_code: LCID (Locale ID) for localized labels and messages. Default is 1033 (English - United States).
   :type language_code: :class:`int`
   :param http_retries: Optional maximum number of retry attempts for transient HTTP errors. Reserved for future use.
   :type http_retries: :class:`int` or None
   :param http_backoff: Optional backoff multiplier (in seconds) between retry attempts. Reserved for future use.
   :type http_backoff: :class:`float` or None
   :param http_timeout: Optional request timeout in seconds. Reserved for future use.
   :type http_timeout: :class:`float` or None
   :param log_config: Optional local HTTP diagnostics logging configuration.
       When provided, all HTTP requests and responses are logged to timestamped
       ``.log`` files with automatic redaction of sensitive headers.
   :type log_config: ~PowerPlatform.Dataverse.core.log_config.LogConfig or None
   :param operation_context: Optional caller-defined context object appended to the
       outbound ``User-Agent`` header as a parenthesized comment. Intended for
       plugin/tool attribution.
   :type operation_context: ~PowerPlatform.Dataverse.core.config.OperationContext or None


   .. py:attribute:: language_code
      :type:  int
      :value: 1033



   .. py:attribute:: http_retries
      :type:  Optional[int]
      :value: None



   .. py:attribute:: http_backoff
      :type:  Optional[float]
      :value: None



   .. py:attribute:: http_timeout
      :type:  Optional[float]
      :value: None



   .. py:attribute:: log_config
      :type:  Optional[PowerPlatform.Dataverse.core.log_config.LogConfig]
      :value: None



   .. py:attribute:: operation_context
      :type:  Optional[OperationContext]
      :value: None



   .. py:method:: from_env() -> DataverseConfig
      :classmethod:


      Create a configuration instance with default settings.

      :return: Configuration instance with default values.
      :rtype: ~PowerPlatform.Dataverse.core.config.DataverseConfig



