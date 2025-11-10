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
            "LogicalName": "account",
            "EntitySetName": "accounts", 
            "PrimaryIdAttribute": "accountid",
            "DisplayName": {"UserLocalizedLabel": {"Label": "Account"}}
        },
        {
            "LogicalName": "contact",
            "EntitySetName": "contacts",
            "PrimaryIdAttribute": "contactid", 
            "DisplayName": {"UserLocalizedLabel": {"Label": "Contact"}}
        }
    ]
}

# Sample OData response for accounts
SAMPLE_ACCOUNTS_RESPONSE = {
    "value": [
        {
            "accountid": "11111111-2222-3333-4444-555555555555",
            "name": "Contoso Ltd",
            "telephone1": "555-0100",
            "websiteurl": "https://contoso.com"
        },
        {
            "accountid": "22222222-3333-4444-5555-666666666666", 
            "name": "Fabrikam Inc",
            "telephone1": "555-0200",
            "websiteurl": "https://fabrikam.com"
        }
    ]
}

# Sample error responses
SAMPLE_ERROR_RESPONSES = {
    "404": {
        "error": {
            "code": "0x80040217",
            "message": "The requested resource was not found."
        }
    },
    "429": {
        "error": {
            "code": "0x80072321", 
            "message": "Too many requests. Please retry after some time."
        }
    }
}

# Sample SQL query results
SAMPLE_SQL_RESPONSE = {
    "value": [
        {"name": "Account 1", "revenue": 1000000},
        {"name": "Account 2", "revenue": 2000000}
    ]
}