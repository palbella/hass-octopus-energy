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
        return True

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
        query = """
            query ($account: String!, $start: DateTime!, $end: DateTime!) {
              account(accountNumber: $account) {
                properties {
                  electricitySupplyPoints {
                    halfHourlyReadings(from: $start, to: $end) {
                      value
                    }
                  }
                }
              }
            }
        """
        now = datetime.now()
        variables = {
            "account": account,
            "start": start.isoformat(),
            "end": now.isoformat()
        }

        headers = {"authorization": self._token}
        client = GraphqlClient(endpoint=GRAPH_QL_ENDPOINT, headers=headers)
        
        try:
            response = await client.execute_async(query, variables)
            _LOGGER.debug(f"Pablo: GraphQL Response for consumption: {response}")
        except Exception as e:
            _LOGGER.debug(f"Pablo: Error fetching consumption: {e}")
            return 0

        total_consumption = 0
        
        if "errors" in response:
            _LOGGER.debug(f"Pablo: GraphQL errors in consumption query: {response['errors']}")
            return 0
        
        try:
            if "data" in response and response["data"] and response["data"]["account"] and response["data"]["account"]["properties"]:
                 for property in response["data"]["account"]["properties"]:
                     if "electricitySupplyPoints" in property:
                         for point in property["electricitySupplyPoints"]:
                             if "halfHourlyReadings" in point:
                                 readings = point["halfHourlyReadings"]
                                 _LOGGER.debug(f"Pablo: Found {len(readings)} readings for point")
                                 for reading in readings:
                                     total_consumption += float(reading["value"])
        except Exception as e:
            _LOGGER.debug(f"Pablo: Error parsing consumption data: {e}, Response: {response}")
            return 0
            
        _LOGGER.debug(f"Pablo: Total calculated consumption: {total_consumption}")             
        return total_consumption
