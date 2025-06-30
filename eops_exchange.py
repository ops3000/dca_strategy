# //eops_strategies/simple_dca_strategy/eops_exchange.py
"""
This module provides the EopsLiveExchange class, which acts as the bridge
between the eops trading framework and the exchange_r backend API.
It implements the BaseExchange interface defined in the eops library.
"""
import requests
import hmac
import hashlib
import time
import json
from typing import Dict, Any, List

from eops.core.exchange import BaseExchange
from eops.utils.logger import log

class EopsLiveExchange(BaseExchange):
    """
    A concrete implementation of BaseExchange that communicates with the
    exchange_r backend via its REST API.
    """
    def __init__(self, params: Dict[str, Any]):
        self.base_url = params.get("base_url")
        self.api_key = params.get("api_key")
        self.secret_key = params.get("secret_key")
        
        if not all([self.base_url, self.api_key, self.secret_key]):
            raise ValueError("base_url, api_key, and secret_key must be provided in EXCHANGE_PARAMS.")
        
        self.session = requests.Session()
        super().__init__(params)

    def _get_auth_headers(self, method: str, path: str, body_str: str = "") -> Dict[str, str]:
        """Generates the required authentication headers for the exchange_r API."""
        timestamp = str(int(time.time()))
        
        # NOTE: The signature format must exactly match the one implemented in api_server's middleware.
        # Here we assume it is: timestamp + method.upper() + path + body
        prehash_string = f"{timestamp}{method.upper()}{path}{body_str}"
        
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            prehash_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest() # Or base64, depending on the server's expectation. Let's assume hex for now.
        
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
            "X-API-TIMESTAMP": timestamp,
            "X-API-SIGN": signature
        }
        
    def connect(self):
        """Checks the connection to the API server."""
        try:
            # A simple way to check connection is to fetch instruments.
            path = "/api/v1/market/instruments"
            response = self.session.get(f"{self.base_url}{path}", timeout=5)
            response.raise_for_status()
            log.info(f"Successfully connected to exchange_r API at {self.base_url}")
        except requests.RequestException as e:
            log.error(f"Failed to connect to exchange_r API: {e}")
            raise

    def create_market_order(self, symbol: str, side: str, amount: float) -> Dict[str, Any]:
        """Places a market order via the API."""
        path = "/api/v1/trade/orders"
        body = {
            "instrument_id": symbol,
            "side": side,
            "order_type": "market",
            "quantity": str(amount) # Ensure quantity is a string to preserve precision
        }
        body_str = json.dumps(body)
        headers = self._get_auth_headers("POST", path, body_str)
        
        try:
            response = self.session.post(f"{self.base_url}{path}", headers=headers, data=body_str, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            log.error(f"Failed to create order: {e}. Response: {e.response.text if e.response else 'N/A'}")
            return {} # Return empty dict on failure

    # get_klines, get_position, get_balance would be implemented similarly
    def get_klines(self, symbol: str, timeframe: str, limit: int) -> List[Dict[str, Any]]:
        # This is more for strategies that need historical data at startup.
        # Real-time data will come from the updater's websocket.
        log.warning("get_klines is not implemented for live trading, as data comes from WebSocket.")
        return []

    def get_position(self, symbol: str) -> Dict[str, float]:
        log.warning("get_position is not yet implemented.")
        return {'long': 0.0, 'short': 0.0}

    def get_balance(self) -> Dict[str, float]:
        log.warning("get_balance is not yet implemented.")
        return {'total': 0.0, 'available': 0.0}