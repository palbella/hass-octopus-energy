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
        # Step 1: Use PropertyType.measurements with confirmed arguments and structure
        
        total_consumption = 0
        now = datetime.now()
        
        # Arguments confirmed via introspection: startAt, endAt
        # Structure confirmed: MeasurementConnection -> edges -> node -> [readAt, value, unit]
        query_measurements = """
            query ($account: String!, $start: DateTime!, $end: DateTime!) {
              account(accountNumber: $account) {
                properties {
                  measurements(startAt: $start, endAt: $end, first: 1000) {
                    edges {
                      node {
                        readAt
                        value
                        unit
                      }
                    }
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
                         connection = property["measurements"]
                         if "edges" in connection:
                             readings = [edge["node"] for edge in connection["edges"]]
                             
                             if readings:
                                 # Sort by date
                                 readings.sort(key=lambda x: x["readAt"])
                                 
                                 start_val = float(readings[0]["value"])
                                 end_val = float(readings[-1]["value"])
                                 
                                 # Calculate difference
                                 diff = end_val - start_val
                                 
                                 # Convert units if needed (Wh to kWh)
                                 first_unit = readings[0].get("unit", "").lower()
                                 if first_unit == "wh":
                                     diff = diff / 1000.0
                                 elif first_unit == "kwh":
                                     pass # already kWh
                                     
                                 if diff > 0:
                                     total_consumption += diff
                                     
        except Exception as e:
            _LOGGER.error(f"Pablo: Error parsing measurements: {e}")
            
        return total_consumption
