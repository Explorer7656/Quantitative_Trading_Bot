import pandas as pd
import numpy as np







def calculate_ev_on_bull_runs(df, tickers, bull_stats, take_profits,
                              lookahead_days=5, stop_loss_pct=0.02, cost_per_trade=0.001):
    results = []
    for ticker in tickers:
        tdf = df[(df['Ticker'] == ticker) & (df['InBullRun'])].sort_values('Date').reset_index(drop=True)
        if tdf.empty:
            continue

        # get median duration from runs summary (fallback=0)
        median_bull = float(bull_stats.loc[bull_stats['Ticker'] == ticker, 'MedianBullDuration'].fillna(0).values[0]) \
                      if 'MedianBullDuration' in bull_stats.columns and not bull_stats.empty else 0.0

        for tp in take_profits:
            wins = losses = total_trades = 0

            for i in range(len(tdf) - lookahead_days):
                entry = float(tdf.loc[i, 'Close'])
                exit_ = float(tdf.loc[i + lookahead_days, 'Close'])

                if exit_ >= entry * (1 + tp):
                    wins += 1
                elif exit_ <= entry * (1 - stop_loss_pct):
                    losses += 1
                total_trades += 1

            if total_trades > 0:
                win_rate = wins / total_trades
                loss_rate = losses / total_trades
                ev_net = (win_rate * tp) - (loss_rate * stop_loss_pct)
            else:
                win_rate = loss_rate = ev_net = 0.0

            # realism check: if typical runs are shorter than the hold horizon, penalize
            ev_realistic = ev_net if median_bull >= lookahead_days else -abs(ev_net)
            ev_with_costs = ev_realistic - (2 * cost_per_trade)

            results.append({
                'Ticker': ticker,
                'TakeProfit': tp,
                'WinRate': win_rate,
                'LossRate': loss_rate,
                'EV_net': ev_net,
                'EV_realistic': ev_realistic,
                'EV_with_costs': ev_with_costs,
                'Trades': total_trades,
                'MedianBullDuration': median_bull
            })
    return pd.DataFrame(results)
