
import asyncio
import logging
from lib.octopus_spain import OctopusSpain
from secret import ACCOUNT_EMAIL, ACCOUNT_PASSWORD

_LOGGER = logging.getLogger(__name__)

async def inspect_schema():
    _LOGGER.debug("Pablo:--- STARTING SCHEMA INSPECTION ---")
    octopus = OctopusSpain(ACCOUNT_EMAIL, ACCOUNT_PASSWORD)
    if not await octopus.login():
        _LOGGER.debug("Pablo:Login failed")
        return

    async def introspect_type(type_name):
        _LOGGER.debug(f"Pablo:\n--- Introspecting {type_name} ---")
        query = f"""
        query {{
          __type(name: "{type_name}") {{
            name
            fields {{
              name
              type {{
                name
                kind
                ofType {{
                  name
                  kind
                }}
              }}
            }}
          }}
        }}
        """
        result = await octopus._client.execute_async(query)
        if "data" in result and result["data"]["__type"] and result["data"]["__type"]["fields"]:
            fields = [f["name"] for f in result["data"]["__type"]["fields"]]
            _LOGGER.debug(f"Pablo:Fields on {type_name}: {sorted(fields)}")
            return fields
        else:
            _LOGGER.debug(f"Pablo:Type {type_name} not found or has no fields. Result: {result}")
            return []

    # 1. Introspect Root Query to find available queries (like electricityMeterReadings vs others)
    _LOGGER.debug("Pablo:\n--- Introspecting Root Query ---")
    query_root = """
    query {
      __schema {
        queryType {
          name
          fields {
            name
          }
        }
      }
    }
    """
    result = await octopus._client.execute_async(query_root)
    if "data" in result and result["data"]["__schema"]["queryType"]:
        fields = [f["name"] for f in result["data"]["__schema"]["queryType"]["fields"]]
        _LOGGER.debug(f"Pablo:Root Query Fields: {sorted(fields)}")
    else:
        _LOGGER.debug("Pablo:Failed to introspect Root Query")

    # 2. Introspect Key Types
    await introspect_type("Account")
    await introspect_type("PropertyType")
    await introspect_type("ElectricitySupplyPoint")
    await introspect_type("Agreement")
    await introspect_type("Meter") # Guessing name
    await introspect_type("ElectricityMeterPoint") # Guessing name

    _LOGGER.debug("Pablo:\n--- END INSPECTION ---")

if __name__ == "__main__":
    asyncio.run(inspect_schema())
