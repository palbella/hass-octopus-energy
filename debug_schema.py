
import asyncio
import logging
from lib.octopus_spain import OctopusSpain
from secret import ACCOUNT_EMAIL, ACCOUNT_PASSWORD

logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)

async def inspect_schema():
    _LOGGER.debug("Starting schema inspection...")
    octopus = OctopusSpain(ACCOUNT_EMAIL, ACCOUNT_PASSWORD)
    if not await octopus.login():
        _LOGGER.debug("Login failed")
        return

    # Introspect PropertyType
    query = """
    query {
      __type(name: "PropertyType") {
        name
        fields {
          name
          type {
            name
            kind
            ofType {
              name
              kind
            }
          }
        }
      }
    }
    """
    
    _LOGGER.debug("Introspecting PropertyType...")
    result = await octopus._client.execute_async(query)
    if "data" in result and result["data"]["__type"]:
        fields = [f["name"] for f in result["data"]["__type"]["fields"]]
        _LOGGER.debug(f"PropertyType fields: {fields}")
    else:
        _LOGGER.debug(f"Failed to introspect PropertyType: {result}")

    # Introspect ElectricitySupplyPoint
    query = """
    query {
      __type(name: "ElectricitySupplyPoint") {
        name
        fields {
          name
          type {
            name
            kind
            ofType {
              name
              kind
            }
          }
        }
      }
    }
    """
    
    _LOGGER.debug("Introspecting ElectricitySupplyPoint...")
    result = await octopus._client.execute_async(query)
    if "data" in result and result["data"]["__type"]:
        fields = [f["name"] for f in result["data"]["__type"]["fields"]]
        _LOGGER.debug(f"ElectricitySupplyPoint fields: {fields}")
    else:
        _LOGGER.debug(f"Failed to introspect ElectricitySupplyPoint: {result}")

if __name__ == "__main__":
    asyncio.run(inspect_schema())
