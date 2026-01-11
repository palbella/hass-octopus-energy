import logging

from datetime import datetime, timedelta

from python_graphql_client import GraphqlClient

_LOGGER = logging.getLogger(__name__)

GRAPH_QL_ENDPOINT = "https://api.oees-kraken.energy/v1/graphql/"
SOLAR_WALLET_LEDGER = "SOLAR_WALLET_LEDGER"
ELECTRICITY_LEDGER = "SPAIN_ELECTRICITY_LEDGER"


class OctopusSpain:
    def __init__(self, email, password):
        self._email = email
        self._password = password
        self._token = None
        self._schema_logged = False

    async def login(self):
        mutation = """
           mutation obtainKrakenToken($input: ObtainJSONWebTokenInput!) {
              obtainKrakenToken(input: $input) {
                token
              }
            }
        """
        variables = {"input": {"email": self._email, "password": self._password}}

        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT)
        response = await client.execute_async(mutation, variables)

        if "errors" in response:
            return False

        self._token = response["data"]["obtainKrakenToken"]["token"]
        
        if not self._schema_logged:
             await self._log_schema_debug_info()
             self._schema_logged = True
             
        return True

    async def _log_schema_debug_info(self):
        _LOGGER.warning("Pablo: STARTING TARGETED SCHEMA INSPECTION (Internal)")
        
        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)

        types_to_inspect = ["SIPSElectricityData", "SIPSElectricityMonthlyConsumption", "Measurement", "MeasurementType", "Reading"]
        
        # 1. Inspect arguments for sipsData query
        query_args = """
        query {
          __type(name: "Query") {
            fields {
              name
              args {
                name
                type {
                  name
                  kind
                }
              }
            }
          }
        }
        """
        try:
            res = await client.execute_async(query_args)
            if "data" in res and res["data"]["__type"] and res["data"]["__type"]["fields"]:
                 for field in res["data"]["__type"]["fields"]:
                     if field["name"] == "sipsData":
                         _LOGGER.warning(f"Pablo: Arguments for sipsData: {[arg['name'] for arg in field['args']]}")
        except Exception as e:
            _LOGGER.warning(f"Pablo: Failed to inspect sipsData args: {e}")

        # 2. Types
        for type_name in types_to_inspect:
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
            try:
                res = await client.execute_async(query)
                if "data" in res and res["data"]["__type"] and res["data"]["__type"]["fields"]:
                     fields = [f["name"] for f in res["data"]["__type"]["fields"]]
                     _LOGGER.warning(f"Pablo: Fields on {type_name}: {sorted(fields)}")
                else:
                     _LOGGER.warning(f"Pablo: Type {type_name} not found or has no fields.")
            except Exception as e:
                _LOGGER.warning(f"Pablo: Failed to inspect {type_name}: {e}")
        
        _LOGGER.warning("Pablo: END TARGETED SCHEMA INSPECTION")

    async def accounts(self):
        query = """
             query getAccountNames{
                viewer {
                    accounts {
                        ... on Account {
                            number
                        }
                    }
                }
            }
            """

        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)
        response = await client.execute_async(query)

        return list(map(lambda a: a["number"], response["data"]["viewer"]["accounts"]))

    async def account(self, account: str):
        query = """
            query ($account: String!) {
              accountBillingInfo(accountNumber: $account) {
                ledgers {
                  ledgerType
                  statementsWithDetails(first: 1) {
                    edges {
                      node {
                        amount
                        consumptionStartDate
                        consumptionEndDate
                        issuedDate
                      }
                    }
                  }
                  balance
                }
              }
            }
        """
        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)
        response = await client.execute_async(query, {"account": account})
        ledgers = response["data"]["accountBillingInfo"]["ledgers"]
        electricity = next(filter(lambda x: x['ledgerType'] == ELECTRICITY_LEDGER, ledgers), None)
        solar_wallet = next(filter(lambda x: x['ledgerType'] == SOLAR_WALLET_LEDGER, ledgers), {'balance': 0})

        if not electricity:
            raise Exception("Electricity ledger not found")

        invoices = electricity["statementsWithDetails"]["edges"]

        if len(invoices) == 0:
            return {
                'solar_wallet': (float(solar_wallet["balance"]) / 100),
                'octopus_credit': (float(electricity["balance"]) / 100),
                'last_invoice': {
                    'amount': None,
                    'issued': None,
                    'start': None,
                    'end': None
                }
            }

        invoice = invoices[0]["node"]

        # Los timedelta son bastante chapuzas, habrá que arreglarlo
        return {
            "solar_wallet": (float(solar_wallet["balance"]) / 100),
            "octopus_credit": (float(electricity["balance"]) / 100),
            "last_invoice": {
                "amount": invoice["amount"] if invoice["amount"] else 0,
                "issued": datetime.fromisoformat(invoice["issuedDate"]).date(),
                "start": (datetime.fromisoformat(invoice["consumptionStartDate"]) + timedelta(hours=2)).date(),
                "end": (datetime.fromisoformat(invoice["consumptionEndDate"]) - timedelta(seconds=1)).date(),
            },
        }

    async def current_consumption(self, account: str, start: datetime):
        # Step 1: Get Meter IDs (Using CUPS as proxy for Meter ID)
        query_cups = """
            query ($account: String!) {
              account(accountNumber: $account) {
                properties {
                  electricitySupplyPoints {
                    cups
                  }
                }
              }
            }
        """
        
        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)
        
        try:
             response_cups = await client.execute_async(query_cups, {"account": account})
        except Exception as e:
             _LOGGER.error(f"Pablo: Error fetching CUPS: {e}")
             return 0

        if "errors" in response_cups:
             _LOGGER.error(f"Pablo: GraphQL errors in CUPS query: {response_cups['errors']}")
             return 0
             
        # Use CUPS as meter identifiers
        meter_ids = []
        try:
            if "data" in response_cups and response_cups["data"]["account"]:
                 for property in response_cups["data"]["account"]["properties"]:
                     if "electricitySupplyPoints" in property:
                         for point in property["electricitySupplyPoints"]:
                             if "cups" in point:
                                 meter_ids.append(point["cups"])
        except Exception as e:
            _LOGGER.error(f"Pablo: Error parsing CUPS: {e}")

        if not meter_ids:
            _LOGGER.error("Pablo: No supply points (CUPS) found for consumption query")
            return 0

        # Step 2: Get Readings for each meter
        total_consumption = 0
        now = datetime.now()
        
        query_readings = """
            query ($account: String!, $meter: String!, $start: DateTime!, $end: DateTime!) {
                electricityMeterReadings(
                    accountNumber: $account
                    meterId: $meter
                    readFrom: $start
                    readTo: $end
                ) {
                    readAt
                    value
                }
            }
        """

        for meter_id in meter_ids:
            variables = {
                "account": account,
                "meter": meter_id,
                "start": start.isoformat(),
                "end": now.isoformat()
            }
            try:
                response = await client.execute_async(query_readings, variables)
                
                if "errors" in response:
                     _LOGGER.error(f"Pablo: Errors fetching readings for meter {meter_id}: {response['errors']}")
                     continue
                
                readings = response.get("data", {}).get("electricityMeterReadings", [])
                if readings:
                    # Sort by date just in case
                    readings.sort(key=lambda x: x["readAt"])
                    
                    # Calculate consumption as difference between last and first reading
                    # Ensure values are floats
                    start_value = float(readings[0]["value"])
                    end_value = float(readings[-1]["value"])
                    
                    consumption = end_value - start_value
                    # Handle potential meter wrap or resets if necessary? 
                    # For now assume simple difference is valid for monthly consumption.
                    if consumption < 0:
                        _LOGGER.warning(f"Pablo: Negative consumption detected for meter {meter_id}: {consumption} (Start: {start_value}, End: {end_value})")
                        consumption = 0 
                        
                    total_consumption += consumption
                    
            except Exception as e:
                _LOGGER.error(f"Pablo: Error fetching readings for meter {meter_id}: {e}")

        return total_consumption
