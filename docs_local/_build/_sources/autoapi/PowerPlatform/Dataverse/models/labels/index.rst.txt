PowerPlatform.Dataverse.models.labels
=====================================

.. py:module:: PowerPlatform.Dataverse.models.labels

.. autoapi-nested-parse::

   Label models for Dataverse metadata.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.labels.LocalizedLabel
   PowerPlatform.Dataverse.models.labels.Label


Module Contents
---------------

.. py:class:: LocalizedLabel

   Represents a localized label with a language code.

   :param label: The text of the label.
   :type label: str
   :param language_code: The language code (LCID), e.g., 1033 for English.
   :type language_code: int
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload. These are merged last and can override default values.
   :type additional_properties: Optional[Dict[str, Any]]


   .. py:attribute:: label
      :type:  str


   .. py:attribute:: language_code
      :type:  int


   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> label = LocalizedLabel(label="Account", language_code=1033)
          >>> label.to_dict()
          {
              '@odata.type': 'Microsoft.Dynamics.CRM.LocalizedLabel',
              'Label': 'Account',
              'LanguageCode': 1033
          }



.. py:class:: Label

   Represents a label that can have multiple localized versions.

   :param localized_labels: List of LocalizedLabel instances.
   :type localized_labels: List[LocalizedLabel]
   :param user_localized_label: Optional user-specific localized label.
   :type user_localized_label: Optional[LocalizedLabel]
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload. These are merged last and can override default values.
   :type additional_properties: Optional[Dict[str, Any]]


   .. py:attribute:: localized_labels
      :type:  List[LocalizedLabel]


   .. py:attribute:: user_localized_label
      :type:  Optional[LocalizedLabel]
      :value: None



   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> label = Label(localized_labels=[LocalizedLabel("Account", 1033)])
          >>> label.to_dict()
          {
              '@odata.type': 'Microsoft.Dynamics.CRM.Label',
              'LocalizedLabels': [
                  {'@odata.type': '...', 'Label': 'Account', 'LanguageCode': 1033}
              ],
              'UserLocalizedLabel': {'@odata.type': '...', 'Label': 'Account', ...}
          }



