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
        
        # Inspect arguments for PropertyType.measurements
        query_args = """
        query {
          __type(name: "PropertyType") {
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
                     if field["name"] == "measurements":
                         _LOGGER.warning(f"Pablo: Arguments for PropertyType.measurements: {[arg['name'] for arg in field['args']]}")
        except Exception as e:
            _LOGGER.warning(f"Pablo: Failed to inspect measurements args: {e}")

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
        # Step 1: Use PropertyType.measurements
        # This is a likely candidate for readings.
        
        total_consumption = 0
        now = datetime.now()
        
        query_measurements = """
            query ($account: String!, $start: DateTime!, $end: DateTime!) {
              account(accountNumber: $account) {
                properties {
                  measurements(from: $start, to: $end) {
                    readAt
                    value
                    unit
                  }
                }
              }
            }
        """

        variables = {
            "account": account,
            "start": start.isoformat(),
            "end": now.isoformat()
        }

        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)
        
        try:
            response = await client.execute_async(query_measurements, variables)
        except Exception as e:
            _LOGGER.error(f"Pablo: Error fetching measurements: {e}")
            return 0

        if "errors" in response:
            _LOGGER.error(f"Pablo: GraphQL errors in measurements query: {response['errors']}")
            return 0
        
        try:
            if "data" in response and response["data"] and response["data"]["account"] and response["data"]["account"]["properties"]:
                 for property in response["data"]["account"]["properties"]:
                     if "measurements" in property and property["measurements"]:
                         measurements = property["measurements"]
                         
                         # Determine if measurements are cumulative or interval
                         # Measurements typically differ by type.
                         # We'll log the first one to debug units.
                         if len(measurements) > 0:
                             first = measurements[0]
                             _LOGGER.warning(f"Pablo: Debug Measurement Sample: {first}")
                            
                             # Logic: If unit is kWh/Wh, sum? Or Start/End diff?
                             # For now, simplistic sum if many items, or diff if cumulative.
                             # If "Reading", likely cumulative?
                             # "Value" in MeasurementType.
                             
                             # Let's try simple difference of first/last if sorted?
                             # Or just sum if it looks like interval data?
                             # Without knowing, I'll assume cumulative readings for now (like a meter).
                             
                             measurements.sort(key=lambda x: x["readAt"])
                             start_val = float(measurements[0]["value"])
                             end_val = float(measurements[-1]["value"])
                             diff = end_val - start_val
                             
                             # Convert units if needed (Wh to kWh)
                             unit = first.get("unit", "").lower()
                             if unit == "wh":
                                 diff = diff / 1000.0
                                 
                             if diff > 0:
                                 total_consumption += diff
                                 
        except Exception as e:
            _LOGGER.error(f"Pablo: Error parsing measurements: {e}")
            
        return total_consumption
