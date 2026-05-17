# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Sample test data and fixtures for Dataverse SDK tests.

This module contains reusable test data, mock responses, and fixtures
that can be used across different test modules.
"""

# Sample entity metadata response
SAMPLE_ENTITY_METADATA = {
    "value": [
        {
            "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
            "LogicalName": "account",
            "SchemaName": "Account",
            "EntitySetName": "accounts",
            "PrimaryIdAttribute": "accountid",
            "DisplayName": {
                "LocalizedLabels": [
                    {
                        "Label": "Account",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                        "MetadataId": "2a4901bf-2241-db11-898a-0007e9e17ebd",
                    },
                ],
                "UserLocalizedLabel": {
                    "Label": "Account",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                    "MetadataId": "2a4901bf-2241-db11-898a-0007e9e17ebd",
                },
            },
            "Description": {
                "LocalizedLabels": [
                    {
                        "Label": "Business that represents a customer or potential customer. The company that is billed in business transactions.",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                        "MetadataId": "294901bf-2241-db11-898a-0007e9e17ebd",
                    },
                ],
                "UserLocalizedLabel": {
                    "Label": "Business that represents a customer or potential customer. The company that is billed in business transactions.",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                    "MetadataId": "294901bf-2241-db11-898a-0007e9e17ebd",
                },
            },
        },
        {
            "MetadataId": "608861bc-50a4-4c5f-a02c-21fe1943e2cf",
            "LogicalName": "contact",
            "SchemaName": "Contact",
            "EntitySetName": "contacts",
            "PrimaryIdAttribute": "contactid",
            "DisplayName": {
                "LocalizedLabels": [
                    {
                        "Label": "Contact",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                        "MetadataId": "3a4901bf-2241-db11-898a-0007e9e17ebd",
                    },
                ],
                "UserLocalizedLabel": {
                    "Label": "Contact",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                    "MetadataId": "3a4901bf-2241-db11-898a-0007e9e17ebd",
                },
            },
            "Description": {
                "LocalizedLabels": [
                    {
                        "Label": "Person with whom a business unit has a relationship, such as customer, supplier, and colleague.",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                    },
                ],
                "UserLocalizedLabel": {
                    "Label": "Person with whom a business unit has a relationship, such as customer, supplier, and colleague.",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                },
            },
        },
    ]
}

# Sample OData response for accounts
SAMPLE_ACCOUNTS_RESPONSE = {
    "value": [
        {
            "accountid": "11111111-2222-3333-4444-555555555555",
            "name": "Contoso Ltd",
            "telephone1": "555-0100",
            "websiteurl": "https://contoso.com",
        },
        {
            "accountid": "22222222-3333-4444-5555-666666666666",
            "name": "Fabrikam Inc",
            "telephone1": "555-0200",
            "websiteurl": "https://fabrikam.com",
        },
    ]
}

# Sample error responses
SAMPLE_ERROR_RESPONSES = {
    "404": {"error": {"code": "0x80040217", "message": "The requested resource was not found."}},
    "429": {"error": {"code": "0x80072321", "message": "Too many requests. Please retry after some time."}},
}

# Sample SQL query results
SAMPLE_SQL_RESPONSE = {"value": [{"name": "Account 1", "revenue": 1000000}, {"name": "Account 2", "revenue": 2000000}]}


# ---------------------------------------------------------------------------
# Column attribute metadata samples
# (Realistic responses from Dataverse Web API, used across unit tests)
# ---------------------------------------------------------------------------

ACCOUNT_NAME_COLUMN = {
    "@odata.type": "#Microsoft.Dynamics.CRM.StringAttributeMetadata",
    "LogicalName": "name",
    "SchemaName": "Name",
    "AttributeType": "String",
    "AttributeTypeName": {"Value": "StringType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": False,
    "IsPrimaryName": True,
    "IsValidForCreate": True,
    "IsValidForUpdate": True,
    "IsValidForRead": True,
    "RequiredLevel": {
        "Value": "ApplicationRequired",
        "CanBeChanged": True,
        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
    },
    "MetadataId": "a1965545-44bc-4b7b-b1ae-93074d0e3f2a",
    "DisplayName": {
        "LocalizedLabels": [
            {
                "Label": "Account Name",
                "LanguageCode": 1033,
                "IsManaged": True,
                "MetadataId": "ea34ed00-2341-db11-898a-0007e9e17ebd",
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Account Name",
            "LanguageCode": 1033,
            "IsManaged": True,
            "MetadataId": "ea34ed00-2341-db11-898a-0007e9e17ebd",
        },
    },
    "Description": {
        "LocalizedLabels": [
            {
                "Label": "Type the company or business name.",
                "LanguageCode": 1033,
                "IsManaged": True,
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Type the company or business name.",
            "LanguageCode": 1033,
            "IsManaged": True,
        },
    },
}

EMAILADDRESS1_COLUMN = {
    "@odata.type": "#Microsoft.Dynamics.CRM.StringAttributeMetadata",
    "LogicalName": "emailaddress1",
    "SchemaName": "EMailAddress1",
    "AttributeType": "String",
    "AttributeTypeName": {"Value": "StringType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": False,
    "IsPrimaryName": False,
    "IsValidForCreate": True,
    "IsValidForUpdate": True,
    "IsValidForRead": True,
    "RequiredLevel": {
        "Value": "None",
        "CanBeChanged": True,
        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
    },
    "MetadataId": "024a2ee3-b983-4fd8-8991-f8d548a227e0",
    "DisplayName": {
        "LocalizedLabels": [
            {
                "Label": "Email",
                "LanguageCode": 1033,
                "IsManaged": True,
                "MetadataId": "54c04ee3-b983-4fd8-8991-f8d548a227e0",
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Email",
            "LanguageCode": 1033,
            "IsManaged": True,
            "MetadataId": "54c04ee3-b983-4fd8-8991-f8d548a227e0",
        },
    },
    "Description": {
        "LocalizedLabels": [
            {
                "Label": "Type the primary email address for the contact.",
                "LanguageCode": 1033,
                "IsManaged": True,
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Type the primary email address for the contact.",
            "LanguageCode": 1033,
            "IsManaged": True,
        },
    },
}

PICKLIST_COLUMN = {
    "@odata.type": "#Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
    "LogicalName": "accountcategorycode",
    "SchemaName": "AccountCategoryCode",
    "AttributeType": "Picklist",
    "AttributeTypeName": {"Value": "PicklistType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": False,
    "IsPrimaryName": False,
    "IsValidForCreate": True,
    "IsValidForUpdate": True,
    "IsValidForRead": True,
    "RequiredLevel": {
        "Value": "None",
        "CanBeChanged": True,
        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
    },
    "MetadataId": "118771ca-6fb9-4f60-8fd4-99b6124b63ad",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Category", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Category", "LanguageCode": 1033, "IsManaged": True},
    },
}

STATUS_COLUMN = {
    "@odata.type": "#Microsoft.Dynamics.CRM.StatusAttributeMetadata",
    "LogicalName": "statuscode",
    "SchemaName": "StatusCode",
    "AttributeType": "Status",
    "AttributeTypeName": {"Value": "StatusType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": False,
    "IsPrimaryName": False,
    "IsValidForCreate": True,
    "IsValidForUpdate": True,
    "IsValidForRead": True,
    "RequiredLevel": {
        "Value": "None",
        "CanBeChanged": True,
        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
    },
    "MetadataId": "f99371c3-b1e1-4645-b2c3-c3db0f59ecf0",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Status Reason", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Status Reason", "LanguageCode": 1033, "IsManaged": True},
    },
}

STATE_COLUMN = {
    "@odata.type": "#Microsoft.Dynamics.CRM.StateAttributeMetadata",
    "LogicalName": "statecode",
    "SchemaName": "StateCode",
    "AttributeType": "State",
    "AttributeTypeName": {"Value": "StateType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": False,
    "IsPrimaryName": False,
    "IsValidForCreate": False,
    "IsValidForUpdate": True,
    "IsValidForRead": True,
    "RequiredLevel": {
        "Value": "SystemRequired",
        "CanBeChanged": False,
        "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
    },
    "MetadataId": "cdc3895a-7539-41e9-966b-3f9ef805aefd",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Status", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Status", "LanguageCode": 1033, "IsManaged": True},
    },
}

UNIQUEID_COLUMN = {
    "LogicalName": "accountid",
    "SchemaName": "AccountId",
    "AttributeType": "Uniqueidentifier",
    "AttributeTypeName": {"Value": "UniqueidentifierType"},
    "IsCustomAttribute": False,
    "IsPrimaryId": True,
    "IsPrimaryName": False,
    "IsValidForCreate": True,
    "IsValidForUpdate": False,
    "IsValidForRead": True,
    "RequiredLevel": {"Value": "SystemRequired"},
    "MetadataId": "f8cd5db9-cee8-4845-8cdd-cd4f504957e7",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Account", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Account", "LanguageCode": 1033, "IsManaged": True},
    },
}

# Reference to the first Account entity from SAMPLE_ENTITY_METADATA,
# with full DisplayName and Description (used in table-get-with-select tests).
ACCOUNT_TABLE_FULL = {k: v for k, v in SAMPLE_ENTITY_METADATA["value"][0].items() if k != "PrimaryIdAttribute"}


# ---------------------------------------------------------------------------
# OptionSet metadata samples
# ---------------------------------------------------------------------------

PICKLIST_OPTIONSET = {
    "MetadataId": "b994cdd8-5ce9-4ab9-bdd3-8888ebdb0407",
    "HasChanged": None,
    "IsCustomOptionSet": False,
    "IsGlobal": False,
    "IsManaged": True,
    "Name": "account_accountcategorycode",
    "OptionSetType": "Picklist",
    "DisplayName": {
        "LocalizedLabels": [
            {
                "Label": "Category",
                "LanguageCode": 1033,
                "IsManaged": True,
                "MetadataId": "d8a3356a-6d26-4f0e-b89e-8b73f25ed57b",
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Category",
            "LanguageCode": 1033,
            "IsManaged": True,
            "MetadataId": "d8a3356a-6d26-4f0e-b89e-8b73f25ed57b",
        },
    },
    "Description": {
        "LocalizedLabels": [
            {
                "Label": "Drop-down list for selecting the category of the account.",
                "LanguageCode": 1033,
                "IsManaged": True,
            }
        ],
        "UserLocalizedLabel": {
            "Label": "Drop-down list for selecting the category of the account.",
            "LanguageCode": 1033,
            "IsManaged": True,
        },
    },
    "Options": [
        {
            "Value": 1,
            "Color": None,
            "IsManaged": True,
            "ExternalValue": None,
            "ParentValues": [],
            "Tag": None,
            "IsHidden": False,
            "Label": {
                "LocalizedLabels": [
                    {
                        "Label": "Preferred Customer",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                        "MetadataId": "0bd8a218-2341-db11-898a-0007e9e17ebd",
                    }
                ],
                "UserLocalizedLabel": {
                    "Label": "Preferred Customer",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                    "MetadataId": "0bd8a218-2341-db11-898a-0007e9e17ebd",
                },
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
        {
            "Value": 2,
            "Color": None,
            "IsManaged": True,
            "ExternalValue": None,
            "ParentValues": [],
            "Tag": None,
            "IsHidden": False,
            "Label": {
                "LocalizedLabels": [
                    {
                        "Label": "Standard",
                        "LanguageCode": 1033,
                        "IsManaged": True,
                        "MetadataId": "0dd8a218-2341-db11-898a-0007e9e17ebd",
                    }
                ],
                "UserLocalizedLabel": {
                    "Label": "Standard",
                    "LanguageCode": 1033,
                    "IsManaged": True,
                    "MetadataId": "0dd8a218-2341-db11-898a-0007e9e17ebd",
                },
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
    ],
}

STATUS_OPTIONSET = {
    "MetadataId": "75ad977d-6f28-4c5c-ae44-7816d366ba21",
    "HasChanged": None,
    "IsCustomOptionSet": False,
    "IsGlobal": False,
    "IsManaged": True,
    "Name": "account_statuscode",
    "OptionSetType": "Status",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Status Reason", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Status Reason", "LanguageCode": 1033, "IsManaged": True},
    },
    "Options": [
        {
            "@odata.type": "#Microsoft.Dynamics.CRM.StatusOptionMetadata",
            "Value": 1,
            "Color": None,
            "IsManaged": True,
            "State": 0,
            "TransitionData": None,
            "Label": {
                "LocalizedLabels": [{"Label": "Active", "LanguageCode": 1033, "IsManaged": True}],
                "UserLocalizedLabel": {"Label": "Active", "LanguageCode": 1033, "IsManaged": True},
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
        {
            "@odata.type": "#Microsoft.Dynamics.CRM.StatusOptionMetadata",
            "Value": 2,
            "Color": None,
            "IsManaged": True,
            "State": 1,
            "TransitionData": None,
            "Label": {
                "LocalizedLabels": [{"Label": "Inactive", "LanguageCode": 1033, "IsManaged": True}],
                "UserLocalizedLabel": {"Label": "Inactive", "LanguageCode": 1033, "IsManaged": True},
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
    ],
}

STATE_OPTIONSET = {
    "MetadataId": "88fa5ad0-2a4b-4281-ac9c-b4e71fb77920",
    "HasChanged": None,
    "IsCustomOptionSet": False,
    "IsGlobal": False,
    "IsManaged": True,
    "Name": "contact_statecode",
    "OptionSetType": "State",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Status", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Status", "LanguageCode": 1033, "IsManaged": True},
    },
    "Options": [
        {
            "@odata.type": "#Microsoft.Dynamics.CRM.StateOptionMetadata",
            "Value": 0,
            "Color": None,
            "IsManaged": True,
            "DefaultStatus": 1,
            "InvariantName": "Active",
            "Label": {
                "LocalizedLabels": [{"Label": "Active", "LanguageCode": 1033, "IsManaged": True}],
                "UserLocalizedLabel": {"Label": "Active", "LanguageCode": 1033, "IsManaged": True},
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
        {
            "@odata.type": "#Microsoft.Dynamics.CRM.StateOptionMetadata",
            "Value": 1,
            "Color": None,
            "IsManaged": True,
            "DefaultStatus": 2,
            "InvariantName": "Inactive",
            "Label": {
                "LocalizedLabels": [{"Label": "Inactive", "LanguageCode": 1033, "IsManaged": True}],
                "UserLocalizedLabel": {"Label": "Inactive", "LanguageCode": 1033, "IsManaged": True},
            },
            "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
        },
    ],
}

BOOLEAN_OPTIONSET = {
    "MetadataId": "0fe276ef-76e9-4121-b570-a09edbf92ab3",
    "HasChanged": None,
    "IsCustomOptionSet": False,
    "IsGlobal": False,
    "IsManaged": True,
    "Name": "contact_donotphone",
    "OptionSetType": "Boolean",
    "DisplayName": {
        "LocalizedLabels": [{"Label": "Do not allow Phone Calls", "LanguageCode": 1033, "IsManaged": True}],
        "UserLocalizedLabel": {"Label": "Do not allow Phone Calls", "LanguageCode": 1033, "IsManaged": True},
    },
    "TrueOption": {
        "Value": 1,
        "Color": None,
        "IsManaged": True,
        "ExternalValue": None,
        "ParentValues": [],
        "Label": {
            "LocalizedLabels": [{"Label": "Do Not Allow", "LanguageCode": 1033, "IsManaged": True}],
            "UserLocalizedLabel": {"Label": "Do Not Allow", "LanguageCode": 1033, "IsManaged": True},
        },
        "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
    },
    "FalseOption": {
        "Value": 0,
        "Color": None,
        "IsManaged": True,
        "ExternalValue": None,
        "ParentValues": [],
        "Label": {
            "LocalizedLabels": [{"Label": "Allow", "LanguageCode": 1033, "IsManaged": True}],
            "UserLocalizedLabel": {"Label": "Allow", "LanguageCode": 1033, "IsManaged": True},
        },
        "Description": {"LocalizedLabels": [], "UserLocalizedLabel": None},
    },
}


# ---------------------------------------------------------------------------
# Relationship metadata samples
# ---------------------------------------------------------------------------

ACCOUNT_CHATS_RELATIONSHIP = {
    "MetadataId": "4c731d0a-8713-f111-8341-7ced8d40bc10",
    "SchemaName": "account_chats",
    "ReferencedAttribute": "accountid",
    "ReferencedEntity": "account",
    "ReferencingAttribute": "regardingobjectid",
    "ReferencingEntity": "chat",
    "RelationshipType": "OneToManyRelationship",
    "IsCustomRelationship": False,
    "IsManaged": False,
    "CascadeConfiguration": {
        "Assign": "Cascade",
        "Delete": "Cascade",
        "Merge": "Cascade",
        "Reparent": "Cascade",
        "Share": "Cascade",
        "Unshare": "Cascade",
    },
}


# ---------------------------------------------------------------------------
# Table list entry fixtures (commonly used in list tests)
# ---------------------------------------------------------------------------

ACCOUNT_TABLE_ENTRY = {
    "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
    "LogicalName": "account",
    "SchemaName": "Account",
    "EntitySetName": "accounts",
}

CONTACT_TABLE_ENTRY = {
    "MetadataId": "608861bc-50a4-4c5f-a02c-21fe1943e2cf",
    "LogicalName": "contact",
    "SchemaName": "Contact",
    "EntitySetName": "contacts",
}
