import alpaca_trade_api as tradeapi
from dotenv import load_dotenv

load_dotenc('env/.env')

class TradingEngine:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        # self.base_url = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
        self.api = tradeapi.REST(api_key, secret_key,
                                 'https://paper-api.alpaca.markets/v2', api_version="v2")

    def get_account_info(self):
        """Return account cash, equity, etc."""
        return self.api.get_account()

    def get_positions(self):
        """List all open positions."""
        return self.api.list_positions()

    def place_order(self, symbol: str, qty: float, side: str, take_profit: float, stop_loss: float):
        """
        Places a bracket order: market entry + take profit + stop loss.
        Example: engine.place_order("AAPL", 1, "buy", 190, 170)
        """
        order = self.api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force="gtc",
            order_class="bracket",
            take_profit={"limit_price": take_profit},
            stop_loss={"stop_price": stop_loss},
        )
        return order

    def cancel_all_orders(self):
        """Cancel all active orders."""
        self.api.cancel_all_orders()

    def close_all_positions(self):
        """Liquidate all positions."""
        self.api.close_all_positions()
