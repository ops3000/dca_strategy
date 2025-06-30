# //eops_strategies/simple_dca_strategy/strategy.py
"""
This file contains the concrete implementations of the UADE pipeline components
for a simple Dollar-Cost Averaging (DCA) strategy.
"""
import asyncio
import json
import websockets
from typing import Set, List

from eops.core.event import Event, EventType
from eops.core.strategy import BaseStrategy
from eops.core.handler import BaseUpdater, BaseDecider, BaseExecutor

# --- 1. Updater: The Data Source ---
class LiveUpdater(BaseUpdater):
    """
    Connects to the exchange_r WebSocket endpoint to receive real-time
    market data and private events like fills.
    """
    def _run(self):
        # The WebSocket connection logic runs in its own async event loop.
        asyncio.run(self._ws_loop())

    async def _ws_loop(self):
        ws_url = self.strategy.params.get("ws_url")
        jwt = self.strategy.params.get("jwt")
        symbol = self.strategy.params.get("symbol")
        
        if not all([ws_url, jwt, symbol]):
            self.log.error("Updater requires 'ws_url', 'jwt', and 'symbol' in STRATEGY_PARAMS.")
            return

        # We connect with the JWT for authentication
        headers = {'Authorization': f'Bearer {jwt}'}
        
        while self.active:
            try:
                async with websockets.connect(ws_url, extra_headers=headers) as websocket:
                    self.log.info(f"Successfully connected to WebSocket at {ws_url}")

                    # Subscribe to the topics we need for this strategy
                    sub_msg = {
                        "op": "subscribe",
                        "args": [
                            f"tickers:{symbol}",
                            # Also subscribe to our own order updates to get fills
                            "private:orders" 
                        ]
                    }
                    await websocket.send(json.dumps(sub_msg))
                    self.log.info(f"Subscribed to topics: {sub_msg['args']}")

                    # Listen for messages
                    async for message in websocket:
                        if not self.active:
                            break
                        self._process_ws_message(message)
                
            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                self.log.error(f"WebSocket connection error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                self.log.error(f"An unexpected error occurred in WebSocket loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    def _process_ws_message(self, message: str):
        """Parses a WebSocket message and puts a corresponding Event on the bus."""
        try:
            data = json.loads(message)
            event_type_str = data.get("event_type")

            if event_type_str == "Ticker":
                # This is a public market data event
                market_event = Event(EventType.MARKET, data=data['data'])
                self.log.debug(f"Dispatching MARKET event: {market_event}")
                self.event_bus.put(market_event)
            
            elif event_type_str == "OrderUpdate":
                # This is a private event about our own orders
                for order_data in data.get("data", []):
                    if order_data.get("status") in ["filled", "partially_filled"]:
                        # For simplicity, we create a FILL event from the order update
                        # A more robust system might have a dedicated FILL event from the server
                        fill_event = Event(EventType.FILL, data=order_data)
                        self.log.info(f"Dispatching FILL event from order update: {fill_event}")
                        self.event_bus.put(fill_event)

        except json.JSONDecodeError:
            self.log.warning(f"Received non-JSON WebSocket message: {message}")
        except Exception as e:
            self.log.error(f"Error processing WebSocket message: {e}", exc_info=True)


# --- 2. Decider: The Brains ---
class DcaDecider(BaseDecider):
    """
    A simple decider that places a buy order on every Nth market tick.
    """
    def __init__(self, strategy: 'BaseStrategy'):
        super().__init__(strategy)
        self.tick_count = 0
        self.buy_interval = self.strategy.params.get("buy_interval_ticks", 5)
        self.buy_amount = self.strategy.params.get("buy_amount", 0.01)
        self.symbol = self.strategy.params.get("symbol")

    @property
    def subscribed_events(self) -> Set[EventType]:
        return {EventType.MARKET}

    def process(self, event: Event):
        self.tick_count += 1
        current_price = event.data.get('price')
        self.log.info(f"Tick {self.tick_count}: Market price for {self.symbol} is {current_price}")

        if self.tick_count % self.buy_interval == 0:
            self.log.info(f"Buy interval reached. Creating ORDER event.")
            order_event = Event(
                EventType.ORDER,
                data={
                    "symbol": self.symbol,
                    "side": "buy",
                    "amount": self.buy_amount
                }
            )
            self.event_bus.put(order_event)

# --- 3. Executor: The Hands ---
class LiveExecutor(BaseExecutor):
    """
    Executes orders by calling the exchange context.
    It doesn't produce FILL events itself; those come from the LiveUpdater.
    """
    @property
    def subscribed_events(self) -> Set[EventType]:
        return {EventType.ORDER}

    def process(self, event: Event):
        order_data = event.data
        self.log.info(f"Executor received ORDER event: {order_data}")
        try:
            # The exchange context is our EopsLiveExchange instance
            self.strategy.context["exchange"].create_market_order(
                symbol=order_data.get("symbol"),
                side=order_data.get("side"),
                amount=order_data.get("amount")
            )
            self.log.info("Order successfully sent to the exchange API.")
        except Exception as e:
            self.log.error(f"Failed to execute order: {e}", exc_info=True)


# --- 4. Strategy: The Composition Root ---
class DcaStrategy(BaseStrategy):
    """
    A simple Dollar-Cost Averaging (DCA) strategy that composes the
    LiveUpdater, DcaDecider, and LiveExecutor.
    """
    def _create_updaters(self) -> List[BaseUpdater]:
        return [LiveUpdater(self)]

    def _create_deciders(self) -> List[BaseDecider]:
        return [DcaDecider(self)]

    def _create_executors(self) -> List[BaseExecutor]:
        return [LiveExecutor(self)]