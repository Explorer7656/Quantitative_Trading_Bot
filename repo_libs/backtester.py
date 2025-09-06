# libs/backtester.py
import backtrader as bt
import pandas as pd
from libs.data_loader import get_stock_data
from libs.bull_detector import detect_and_label_bull_runs
import numpy as np
import matplotlib.pyplot as plt



# 1) Strategy
# -----------------------------
class SMACrossBullStrategy(bt.Strategy):
    params = (
        ("short_window", 5),
        ("long_window", 20),
    )

    def __init__(self):
        self.short_sma = bt.ind.SMA(period=self.p.short_window)
        self.long_sma = bt.ind.SMA(period=self.p.long_window)
        self.wins = []
        self.losses = []
        self.trade_pnls = []
        self.equity_curve = []
        self.order = None


    def next(self):
        self.equity_curve.append(self.broker.getvalue())
        in_bull = int(self.data.InBullRun[0])

        if not self.position:
            if in_bull and self.short_sma[0] > self.long_sma[0]:
                # --- dynamic sizing ---
                cash = self.broker.getcash()
                allocation = 0.7  # use 50% of portfolio per trade
                invest_amount = cash * allocation
                size = int(invest_amount / self.data.close[0])

                if size > 0:
                    self.buy(size=size)
                    print(f"BUY {size} shares on {self.data.datetime.date(0)} @ {self.data.close[0]} "
                          f"(using {allocation * 100:.0f}% of portfolio)")
        else:
            if not in_bull or self.short_sma[0] < self.long_sma[0]:
                self.close()
                print(f"CLOSE on {self.data.datetime.date(0)} @ {self.data.close[0]}")


    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f"BUY EXECUTED @ {order.executed.price}")
            elif order.issell():
                print(f"SELL EXECUTED @ {order.executed.price}")
            self.order = None  # reset the pending order tracker
        elif order.status in [order.Canceled, order.Rejected]:
            print("Order canceled or rejected")
            self.order = None
    def log(self, txt):
        dt = self.data.datetime.date(0)
        print(f"{dt} {txt}")

    def notify_trade(self, trade):
        if trade.isclosed:
            pnl = trade.pnlcomm
            self.trade_pnls.append(pnl)
            if pnl > 0:
                self.wins.append(pnl)
            else:
                self.losses.append(abs(pnl))
            self.log(f"Closed trade PnL: {pnl:.2f}")

    def stop(self):

        if self.wins and self.losses:
            avg_win = sum(self.wins) / len(self.wins)
            avg_loss = sum(self.losses) / len(self.losses)
            pl_ratio = avg_win / avg_loss if avg_loss != 0 else float('inf')
            report_performance(self.wins, self.losses, self.trade_pnls, self.equity_curve)
            print(f"Avg Win: {avg_win:.2f}, Avg Loss: {avg_loss:.2f}, Profit/Loss Ratio: {pl_ratio:.2f}")
        else:
            print("No completed trades to calculate profit/loss ratio.")

def plot_equity_curve(equity_curve):

    plt.plot(equity_curve)
    plt.title("Equity Curve")
    plt.xlabel("Bar Number")
    plt.ylabel("Portfolio Value")
    plt.show()

def report_performance(wins, losses, trade_pnls, equity_curve):
    total_trades = len(trade_pnls)
    if total_trades == 0:
        print("No trades were closed.")
        return

    # Avg Win/Loss
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')

    # Win Rate
    win_rate = len(wins) / total_trades * 100

    # Expectancy
    expectancy = np.mean(trade_pnls)

    # Max Drawdown
    eq = np.array(equity_curve)
    if len(eq) > 1:
        cummax = np.maximum.accumulate(eq)
        dd = (cummax - eq) / cummax
        max_dd = dd.max() * 100
    else:
        max_dd = 0

    # Sharpe Ratio (daily)
    returns = np.diff(eq) / eq[:-1]
    sharpe = (np.mean(returns) / np.std(returns) * np.sqrt(252)) if np.std(returns) > 0 else 0

    # Print Report
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Avg Win: {avg_win:.2f}, Avg Loss: {avg_loss:.2f}, P/L Ratio: {pl_ratio:.2f}")
    print(f"Expectancy (per trade): {expectancy:.2f}")
    print(f"Max Drawdown: {max_dd:.2f}%")
    print(f"Sharpe Ratio: {sharpe:.2f}")


# -----------------------------
# 2) Load data
# -----------------------------
tickers = ["AAPL", "MSFT", "TSLA", 'OPAI.PVT', 'BAC']
df = get_stock_data(tickers, start="2022-01-01", end="2025-01-01")

# Use only one ticker at a time for simplicity in backtrader
df_clean, runs_df = detect_and_label_bull_runs(
    df, tickers,
    trend_window=30,
    slope_threshold_ppd=0.0001,
    min_bull_duration_days=10,
    use_log=True
)

df_ticker = df_clean[df_clean['Ticker'] == "TSLA"].copy()
df_ticker = df_ticker.sort_values("Date")
df_ticker.set_index("Date", inplace=True)

# Make sure InBullRun is 0/1 (Backtrader prefers numeric)
df_ticker['InBullRun'] = df_ticker['InBullRun'].astype(int)
print(df_clean[df_clean['Ticker']=='TSLA'][['Date','Close','InBullRun']].tail(30))


# df_ticker = df[df['Ticker'] == "AAPL"].copy()
# df_ticker = df_ticker.sort_values("Date")
# df_ticker.set_index("Date", inplace=True)

# -----------------------------
# 3) Create Backtrader feed
# -----------------------------
class PandasDaily(bt.feeds.PandasData):
    lines = ('InBullRun',)
    params = (
        ('datetime', None),
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('close', 'Close'),
        ('volume', None),
        ('openinterest', None),
        ('InBullRun', 'InBullRun')
    )

data_feed = PandasDaily(dataname=df_ticker)

# -----------------------------
# 4) Cerebro setup
# -----------------------------
cerebro = bt.Cerebro()
cerebro.addstrategy(SMACrossBullStrategy)
cerebro.adddata(data_feed)
cerebro.broker.set_cash(100000)
cerebro.broker.setcommission(commission=0.001)

# -----------------------------
# 5) Run
# -----------------------------
start=cerebro.broker.getvalue()
print(f"Starting cash: {cerebro.broker.getvalue():.2f}")
cerebro.run()
print(f"Ending cash: {cerebro.broker.getvalue():.2f}")
end=cerebro.broker.getvalue()
print(f"profit:{end-start}")
# -----------------------------
# 6) Plot
# -----------------------------
cerebro.plot(style='candlestick', volume=False)

results = cerebro.run()
strat = results[0]  # first strategy instance
plot_equity_curve(strat.equity_curve)

