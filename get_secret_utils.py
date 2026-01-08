# get_secret_utils.py
# my libs
from app_config import ONEP_HEADER
from onepassword.client import Client

"""

name:  get_secret_utils.py
purpose:

"""

async def get_all_1p_vaults_and_items():
    """
    Get all vaults and items with their IDs and types
    """
    # Authenticate with service account token
    client = await Client.authenticate(**ONEP_HEADER)

    results = []

    # Get all vaults
    vaults = await client.vaults.list()

    # Add each vault to results
    for vault in vaults:
        results.append({
            'id': vault.id,
            'type': 'vault',
            'name': vault.title.strip()
        })

        # Get all items in this vault
        items = await client.items.list(vault.id)

        # Add each item to results
        for item in items:
            results.append({
                'id': item.id,
                'type': 'item',
                'name': item.title.strip()
            })

    return results



async def get_1p_secret(vault_id, item_id):
    # Authenticate with service account token if provided

    client = await Client.authenticate(**ONEP_HEADER)

    # Get full item with fields
    full_item = await client.items.get(vault_id, item_id)

    # # Print field names
    # print("\nAvailable fields:")
    # for field in full_item.fields:
    #     print(f"  - {field.title}")

    # Return all fields as dictionary
    return {field.title: field.value for field in full_item.fields}



#old method
async def get_1p_secrets(vault, item):
    # Authenticate with service account token if provided

    onep_header = {**ONEP_HEADER}

    client = await Client.authenticate(**ONEP_HEADER)

    # Get vault
    vaults = await client.vaults.list()

    # # DEBUG: Print all vaults to see what's available
    # print(f"\nVaults '{vault}':")
    # for i in vaults:
    #     print(f"  - '{i.title}'")

    vault_name= next((v for v in vaults if v.title == vault), None)
    if not vault_name:
        raise ValueError(f"Vault '{vault}' not found")

    # Get item
    items = await client.items.list(vault_name.id)

    # # DEBUG: Print all items to see what's available
    # print(f"\nItems in vault '{vault_name}':")
    # for i in items:
    #     print(f"  - '{i.title}'")

    item_name = next((i for i in items if i.title.strip() == item.strip()), None)
    if not item_name:
        raise ValueError(f"Item '{item}' not found")

    # Get full item with fields
    full_item = await client.items.get(vault_name.id, item_name.id)

    # Print field names
    print("\nAvailable fields:")
    for field in full_item.fields:
        print(f"  - {field.title}")

    # Return all fields as dictionary
    return {field.title: field.value for field in full_item.fields}
    return