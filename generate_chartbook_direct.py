"""
Direct Bloomberg Chartbook Generator for S&P 500.
Bypasses Excel for faster performance by pulling data directly from Bloomberg API.
Automatically fetches current S&P 500 constituents from Bloomberg.

INTERACTIVE PLOTLY VERSION:
- Session reuse (single Bloomberg session for all requests)
- Batch API requests (multiple securities per request)
- Interactive Plotly charts with lazy loading
"""

import os
import sys
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# Configuration
BASE_DIR = Path(r"C:\Users\JaySheth\OneDrive - Solaria Management, LP\Desktop\Claude Code\Stock Price vs Target")
YEARS_FOR_PERCENTILES = 10  # Years of data to calculate percentiles
YEARS_TO_PLOT = 5  # Years to show on chart
HTML_OUTPUT = BASE_DIR / "chartbook_direct.html"
PORTFOLIOS_FILE = BASE_DIR / "custom_portfolios.json"


def check_bloomberg_available():
    """Check if Bloomberg API is available."""
    try:
        import blpapi
        return True
    except ImportError:
        return False


class BloombergSession:
    """Manages a single Bloomberg session for all requests."""

    def __init__(self):
        import blpapi
        self.blpapi = blpapi
        self.session = None
        self.refDataService = None

    def start(self):
        """Start the Bloomberg session."""
        sessionOptions = self.blpapi.SessionOptions()
        sessionOptions.setServerHost("localhost")
        sessionOptions.setServerPort(8194)

        self.session = self.blpapi.Session(sessionOptions)
        if not self.session.start():
            raise Exception("Failed to start Bloomberg session")

        if not self.session.openService("//blp/refdata"):
            raise Exception("Failed to open //blp/refdata")

        self.refDataService = self.session.getService("//blp/refdata")

    def stop(self):
        """Stop the Bloomberg session."""
        if self.session:
            self.session.stop()
            self.session = None
            self.refDataService = None

    def get_sp500_constituents(self):
        """
        Fetch current S&P 500 constituents.
        Returns: list of tickers
        """
        request = self.refDataService.createRequest("ReferenceDataRequest")
        request.getElement("securities").appendValue("SPX Index")
        request.getElement("fields").appendValue("INDX_MWEIGHT")

        self.session.sendRequest(request)

        tickers = []
        while True:
            event = self.session.nextEvent(5000)
            for msg in event:
                if msg.hasElement("securityData"):
                    securityData = msg.getElement("securityData")
                    if securityData.numValues() > 0:
                        sec = securityData.getValueAsElement(0)
                        if sec.hasElement("fieldData"):
                            fieldData = sec.getElement("fieldData")
                            if fieldData.hasElement("INDX_MWEIGHT"):
                                membersData = fieldData.getElement("INDX_MWEIGHT")
                                for i in range(membersData.numValues()):
                                    member = membersData.getValueAsElement(i)
                                    if member.hasElement("Member Ticker and Exchange Code"):
                                        ticker_exchange = member.getElementAsString("Member Ticker and Exchange Code")
                                        ticker = ticker_exchange.split()[0]
                                        tickers.append(ticker)
            if event.eventType() == self.blpapi.Event.RESPONSE:
                break

        return list(set(tickers))

    def get_metadata_batch(self, tickers, batch_size=200):
        """
        Fetch company names, GICS sectors, and market caps in a single batch request.
        Returns: (company_names_dict, gics_sectors_dict, market_caps_dict)
        """
        company_names = {}
        gics_sectors = {}
        market_caps = {}
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"  Batch {batch_num}/{total_batches} ({len(batch)} tickers)...", end=" ", flush=True)

            request = self.refDataService.createRequest("ReferenceDataRequest")
            for ticker in batch:
                request.getElement("securities").appendValue(f"{ticker} Equity")
            request.getElement("fields").appendValue("NAME")
            request.getElement("fields").appendValue("GICS_SECTOR_NAME")
            request.getElement("fields").appendValue("CUR_MKT_CAP")

            self.session.sendRequest(request)

            while True:
                event = self.session.nextEvent(5000)
                for msg in event:
                    if msg.hasElement("securityData"):
                        securityData = msg.getElement("securityData")
                        for j in range(securityData.numValues()):
                            sec = securityData.getValueAsElement(j)
                            security = sec.getElementAsString("security")
                            ticker = security.removesuffix(" Equity")

                            if sec.hasElement("fieldData"):
                                fieldData = sec.getElement("fieldData")
                                if fieldData.hasElement("NAME"):
                                    company_names[ticker] = fieldData.getElementAsString("NAME")
                                else:
                                    company_names[ticker] = ticker
                                if fieldData.hasElement("GICS_SECTOR_NAME"):
                                    gics_sectors[ticker] = fieldData.getElementAsString("GICS_SECTOR_NAME")
                                else:
                                    gics_sectors[ticker] = "Unknown"
                                if fieldData.hasElement("CUR_MKT_CAP"):
                                    market_caps[ticker] = fieldData.getElementAsFloat("CUR_MKT_CAP")
                                else:
                                    market_caps[ticker] = 0
                            else:
                                company_names[ticker] = ticker
                                gics_sectors[ticker] = "Unknown"
                                market_caps[ticker] = 0
                if event.eventType() == self.blpapi.Event.RESPONSE:
                    break
            print("done")

        for ticker in tickers:
            if ticker not in company_names:
                company_names[ticker] = ticker
            if ticker not in gics_sectors:
                gics_sectors[ticker] = "Unknown"
            if ticker not in market_caps:
                market_caps[ticker] = 0

        return company_names, gics_sectors, market_caps

    def get_historical_data_batch(self, tickers, years=10, batch_size=25):
        """Fetch historical data for multiple tickers in batches.

        Sends all batch requests upfront, then collects all responses
        in a single event loop using correlation IDs to match them.
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)

        # Split tickers into batches and send all requests immediately
        batches = []
        for i in range(0, len(tickers), batch_size):
            batches.append(tickers[i:i + batch_size])

        total_batches = len(batches)
        print(f"  Sending {total_batches} batch requests upfront...")

        for idx, batch in enumerate(batches):
            cid = self.blpapi.CorrelationId(idx)
            request = self.refDataService.createRequest("HistoricalDataRequest")
            for ticker in batch:
                request.getElement("securities").appendValue(f"{ticker} Equity")
            request.getElement("fields").appendValue("PX_LAST")
            request.getElement("fields").appendValue("BEST_TARGET_PRICE")
            request.set("startDate", start_date.strftime("%Y%m%d"))
            request.set("endDate", end_date.strftime("%Y%m%d"))
            request.set("periodicitySelection", "DAILY")
            self.session.sendRequest(request, correlationId=cid)

        # Collect all responses in a single event loop
        results = {}
        batches_done = 0

        while batches_done < total_batches:
            event = self.session.nextEvent(60000)
            for msg in event:
                if msg.hasElement("securityData"):
                    securityData = msg.getElement("securityData")
                    security = securityData.getElementAsString("security")
                    ticker = security.removesuffix(" Equity")

                    if securityData.hasElement("fieldData"):
                        fieldData = securityData.getElement("fieldData")

                        dates = []
                        prices = []
                        targets = []

                        for k in range(fieldData.numValues()):
                            point = fieldData.getValueAsElement(k)
                            date = point.getElementAsDatetime("date")
                            dates.append(datetime(date.year, date.month, date.day))

                            if point.hasElement("PX_LAST"):
                                prices.append(point.getElementAsFloat("PX_LAST"))
                            else:
                                prices.append(np.nan)

                            if point.hasElement("BEST_TARGET_PRICE"):
                                targets.append(point.getElementAsFloat("BEST_TARGET_PRICE"))
                            else:
                                targets.append(np.nan)

                        if dates:
                            df = pd.DataFrame({
                                'price': prices,
                                'target': targets
                            }, index=pd.DatetimeIndex(dates))
                            df['target'] = df['target'].ffill()
                            df = df.dropna()
                            results[ticker] = df

            if event.eventType() == self.blpapi.Event.RESPONSE:
                batches_done += 1
                print(f"  Batch {batches_done}/{total_batches} complete ({len(results)} total datasets)")

        return results


def calculate_percentile_bands(df, percentile_low=10, percentile_high=90):
    """Calculate the price/target ratio percentiles and bands."""
    df = df.copy()
    df['ratio'] = df['price'] / df['target']
    ratio_p_low = df['ratio'].quantile(percentile_low / 100)
    ratio_p_high = df['ratio'].quantile(percentile_high / 100)
    df['band_low'] = ratio_p_low * df['target']
    df['band_high'] = ratio_p_high * df['target']
    return df, ratio_p_low, ratio_p_high


def create_plotly_chart_json(df, ticker, company_name, years_to_plot=5):
    """Create Plotly chart data as JSON for lazy loading."""
    cutoff_date = datetime.now() - timedelta(days=years_to_plot * 365)
    plot_df = df[df.index >= cutoff_date].copy()

    # Use ISO dates with noon time to avoid timezone/midnight issues
    dates = [d.strftime('%Y-%m-%dT12:00:00') for d in plot_df.index]

    # Formatted date strings for hover display
    date_labels = [f'{d.month}/{d.day}/{d.year}' for d in plot_df.index]

    # Create a set of actual trading dates for quick lookup
    trading_dates = set(plot_df.index)

    # Calculate tick dates at 6-month intervals from most recent data point
    last_date = plot_df.index.max().to_pydatetime()
    first_date = plot_df.index.min().to_pydatetime()

    def find_nearest_trading_day(target_date, trading_dates, direction='backward'):
        """Find the nearest trading day to target_date."""
        target_ts = pd.Timestamp(target_date)
        if target_ts in trading_dates:
            return target_date

        for offset in range(1, 8):
            if direction == 'backward':
                check_date = target_date - timedelta(days=offset)
            else:
                check_date = target_date + timedelta(days=offset)
            check_ts = pd.Timestamp(check_date)
            if check_ts in trading_dates:
                return check_date

        for offset in range(1, 8):
            if direction == 'backward':
                check_date = target_date + timedelta(days=offset)
            else:
                check_date = target_date - timedelta(days=offset)
            check_ts = pd.Timestamp(check_date)
            if check_ts in trading_dates:
                return check_date

        return target_date

    tick_dates = [last_date]
    current = last_date

    while True:
        month = current.month - 6
        year = current.year
        if month <= 0:
            month += 12
            year -= 1
        try:
            target = current.replace(year=year, month=month)
        except ValueError:
            target = current.replace(year=year, month=month, day=28)

        if target < first_date:
            break

        actual_date = find_nearest_trading_day(target, trading_dates, 'backward')
        tick_dates.append(actual_date)
        current = target

    tick_dates = sorted(tick_dates)
    tick_vals = [d.strftime('%Y-%m-%dT12:00:00') for d in tick_dates]
    tick_text = [f'{d.month}/{d.day}/{d.year}' for d in tick_dates]

    chart_data = {
        'dates': dates,
        'date_labels': date_labels,
        'price': plot_df['price'].round(2).tolist(),
        'band_low': plot_df['band_low'].round(2).tolist(),
        'band_high': plot_df['band_high'].round(2).tolist(),
        'ticker': ticker,
        'company_name': company_name,
        'tick_vals': tick_vals,
        'tick_text': tick_text
    }

    return chart_data


def generate_chart_data(ticker, df, company_name, years_to_plot):
    """Generate chart data for a single ticker."""
    try:
        if len(df) < 100:
            return None, f"insufficient data ({len(df)} days)"

        df, p_low, p_high = calculate_percentile_bands(df)
        chart_json = create_plotly_chart_json(df, ticker, company_name, years_to_plot)
        return chart_json, None
    except Exception as e:
        return None, str(e)


def load_custom_portfolios():
    """Load custom portfolios from JSON file. Returns (portfolios_list, custom_tickers_set)."""
    portfolios = []
    custom_tickers = set()
    if PORTFOLIOS_FILE.exists():
        try:
            with open(PORTFOLIOS_FILE, 'r') as f:
                data = json.load(f)
            portfolios = data.get('portfolios', [])
            for p in portfolios:
                for ticker in p.get('tickers', []):
                    custom_tickers.add(ticker.upper())
            print(f"  Loaded {len(portfolios)} custom portfolios with {len(custom_tickers)} unique tickers")
        except Exception as e:
            print(f"  Warning: Could not load {PORTFOLIOS_FILE}: {e}")
    else:
        print(f"  No custom portfolios file found (will create on first export)")
    return portfolios, custom_tickers


def generate_html_chartbook(chart_data_list, output_file, portfolios=None):
    """Generate an HTML file with interactive Plotly charts and lazy loading."""
    if portfolios is None:
        portfolios = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get unique sectors and sort them
    sectors = sorted(set(item['sector'] for item in chart_data_list))

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>S&P 500 Chartbook - Stock Price vs Target</title>
    <script src="https://cdn.plot.ly/plotly-basic-2.27.0.min.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            padding: 20px;
        }}
        header {{
            text-align: center;
            padding: 30px 20px;
            margin-bottom: 30px;
            background: linear-gradient(135deg, #16213e 0%, #1a1a2e 100%);
            border-radius: 12px;
            border: 1px solid #0f3460;
        }}
        h1 {{ font-size: 2rem; margin-bottom: 10px; color: #e94560; }}
        .subtitle {{ color: #888; }}
        .search-container {{
            max-width: 600px;
            margin: 20px auto;
        }}
        #searchInput {{
            width: 100%;
            padding: 12px 20px;
            font-size: 16px;
            border: 2px solid #0f3460;
            border-radius: 8px;
            background: #16213e;
            color: #eee;
        }}
        #searchInput:focus {{
            outline: none;
            border-color: #e94560;
        }}
        .sort-buttons {{
            display: flex;
            justify-content: center;
            gap: 10px;
            margin: 20px auto;
        }}
        .sort-btn {{
            padding: 10px 20px;
            font-size: 14px;
            border: 2px solid #0f3460;
            border-radius: 8px;
            background: #16213e;
            color: #eee;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .sort-btn:hover {{
            border-color: #e94560;
        }}
        .sort-btn.active {{
            background: #e94560;
            border-color: #e94560;
        }}
        .sector-filters {{
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 8px;
            margin-top: 15px;
            max-width: 900px;
            margin-left: auto;
            margin-right: auto;
        }}
        .sector-btn {{
            padding: 6px 12px;
            font-size: 12px;
            border: 1px solid #0f3460;
            border-radius: 4px;
            background: #16213e;
            color: #aaa;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .sector-btn:hover {{
            border-color: #e94560;
            color: #eee;
        }}
        .sector-btn.active {{
            background: #0f3460;
            border-color: #4ade80;
            color: #4ade80;
        }}
        .filter-label {{
            color: #666;
            font-size: 12px;
            margin-top: 15px;
        }}
        .signal-filters {{
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 8px;
            margin-top: 10px;
            max-width: 900px;
            margin-left: auto;
            margin-right: auto;
        }}
        .signal-btn {{
            padding: 6px 14px;
            font-size: 12px;
            border: 1px solid #0f3460;
            border-radius: 4px;
            background: #16213e;
            color: #aaa;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .signal-btn:hover {{
            border-color: #e94560;
            color: #eee;
        }}
        .signal-btn.active {{
            background: #0f3460;
        }}
        .signal-btn.active[data-signal="all"] {{
            border-color: #4ade80;
            color: #4ade80;
        }}
        .signal-btn.active[data-signal="breakout"] {{
            border-color: #f97316;
            color: #f97316;
        }}
        .signal-btn.active[data-signal="breakdown"] {{
            border-color: #38bdf8;
            color: #38bdf8;
        }}
        .portfolio-section {{
            margin-top: 20px;
            border-top: 1px solid #0f3460;
            padding-top: 15px;
        }}
        .portfolio-header {{
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
        }}
        .portfolio-filters {{
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 8px;
            max-width: 900px;
            margin-left: auto;
            margin-right: auto;
        }}
        .portfolio-btn {{
            padding: 6px 12px;
            font-size: 12px;
            border: 1px solid #0f3460;
            border-radius: 4px;
            background: #16213e;
            color: #aaa;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .portfolio-btn:hover {{
            border-color: #e94560;
            color: #eee;
        }}
        .portfolio-btn.active {{
            background: #0f3460;
            border-color: #c084fc;
            color: #c084fc;
        }}
        .portfolio-btn .delete-portfolio {{
            margin-left: 6px;
            color: #666;
            font-size: 10px;
            cursor: pointer;
        }}
        .portfolio-btn .delete-portfolio:hover {{
            color: #e94560;
        }}
        .create-portfolio-btn {{
            padding: 6px 14px;
            font-size: 12px;
            border: 1px dashed #c084fc;
            border-radius: 4px;
            background: transparent;
            color: #c084fc;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .create-portfolio-btn:hover {{
            background: #1e1e3e;
            border-style: solid;
        }}
        .portfolio-modal {{
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }}
        .portfolio-modal.visible {{
            display: flex;
        }}
        .modal-content {{
            background: #16213e;
            border: 1px solid #0f3460;
            border-radius: 12px;
            padding: 25px;
            width: 450px;
            max-width: 90vw;
            max-height: 80vh;
            overflow-y: auto;
        }}
        .modal-content h3 {{
            color: #c084fc;
            margin-bottom: 15px;
            font-size: 1.1rem;
        }}
        .modal-content label {{
            display: block;
            color: #aaa;
            font-size: 12px;
            margin-bottom: 5px;
            margin-top: 12px;
        }}
        .modal-content input[type="text"] {{
            width: 100%;
            padding: 8px 12px;
            font-size: 14px;
            border: 1px solid #0f3460;
            border-radius: 6px;
            background: #1a1a2e;
            color: #eee;
        }}
        .modal-content input[type="text"]:focus {{
            outline: none;
            border-color: #c084fc;
        }}
        .modal-actions {{
            display: flex;
            justify-content: flex-end;
            gap: 10px;
            margin-top: 20px;
        }}
        .modal-actions button {{
            padding: 8px 18px;
            font-size: 13px;
            border-radius: 6px;
            cursor: pointer;
            border: 1px solid #0f3460;
        }}
        .btn-cancel {{
            background: transparent;
            color: #aaa;
        }}
        .btn-cancel:hover {{
            color: #eee;
        }}
        .btn-save {{
            background: #c084fc;
            color: #1a1a2e;
            border-color: #c084fc;
            font-weight: bold;
        }}
        .btn-save:hover {{
            background: #a855f7;
        }}
        .ticker-chips {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 8px;
            min-height: 30px;
        }}
        .ticker-chip {{
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 3px 8px;
            background: #0f3460;
            border-radius: 4px;
            font-size: 11px;
            color: #eee;
            font-family: monospace;
        }}
        .ticker-chip .remove-ticker {{
            cursor: pointer;
            color: #888;
            font-size: 10px;
        }}
        .ticker-chip .remove-ticker:hover {{
            color: #e94560;
        }}
        .modal-hint {{
            color: #555;
            font-size: 11px;
            margin-top: 4px;
        }}
        .modal-hint.warn {{
            color: #f59e0b;
        }}
        .portfolio-btn .edit-portfolio {{
            margin-left: 4px;
            color: #666;
            font-size: 10px;
            cursor: pointer;
        }}
        .portfolio-btn .edit-portfolio:hover {{
            color: #c084fc;
        }}
        .portfolio-btn[draggable="true"] {{
            cursor: grab;
        }}
        .portfolio-btn.dragging {{
            opacity: 0.4;
        }}
        .portfolio-btn.drag-over {{
            border-color: #c084fc;
            box-shadow: 0 0 6px rgba(192, 132, 252, 0.5);
        }}
        .export-portfolio-btn {{
            padding: 6px 14px;
            font-size: 12px;
            border: 1px dashed #4ade80;
            border-radius: 4px;
            background: transparent;
            color: #4ade80;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .export-portfolio-btn:hover {{
            background: #1e1e3e;
            border-style: solid;
        }}
        .charts-container {{ max-width: 1200px; margin: 0 auto; }}
        .chart-card {{
            background: #16213e;
            border-radius: 12px;
            margin-bottom: 30px;
            overflow: hidden;
            border: 1px solid #0f3460;
        }}
        .chart-card.hidden {{ display: none; }}
        .chart-header {{
            padding: 20px;
            border-bottom: 1px solid #0f3460;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .ticker {{ font-size: 1.5rem; font-weight: bold; font-family: monospace; color: #e94560; }}
        .company-name {{ color: #888; font-size: 0.9rem; margin-left: 15px; }}
        .market-cap {{ color: #4ade80; font-size: 0.85rem; margin-left: 10px; }}
        .sector-tag {{ color: #60a5fa; font-size: 0.75rem; margin-left: 10px; background: #1e3a5f; padding: 2px 8px; border-radius: 4px; }}
        .chart-number {{ color: #666; font-size: 0.9rem; }}
        .chart-container {{
            width: 100%;
            height: 550px;
            background: #fff;
        }}
        .chart-placeholder {{
            width: 100%;
            height: 550px;
            background: #f0f0f0;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #666;
            font-size: 14px;
        }}
        .loading-spinner {{
            width: 40px;
            height: 40px;
            border: 3px solid #ddd;
            border-top-color: #e94560;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }}
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
        footer {{ text-align: center; padding: 40px 20px; color: #666; }}
        .back-to-top {{
            display: block;
            text-align: center;
            padding: 12px;
            color: #e94560;
            text-decoration: none;
            font-size: 13px;
            border-top: 1px solid #0f3460;
            transition: background 0.2s;
        }}
        .back-to-top:hover {{
            background: #0f3460;
        }}
    </style>
</head>
<body>
    <header id="top">
        <h1>S&P 500 Chartbook</h1>
        <p class="subtitle">Stock Price vs Analyst Target Bands - Generated {timestamp}</p>
        <p class="subtitle">{len(chart_data_list)} interactive charts with lazy loading</p>
        <div class="search-container">
            <input type="text" id="searchInput" placeholder="Search by ticker or company name..." onkeyup="filterCharts()">
        </div>
        <div class="sort-buttons">
            <button class="sort-btn active" onclick="sortByTicker()">Sort A-Z</button>
            <button class="sort-btn" onclick="sortByMarketCap()">Sort by Market Cap</button>
        </div>
        <p class="filter-label">Filter by GICS Sector (click multiple to combine):</p>
        <div class="sector-filters">
            <button class="sector-btn active" onclick="filterBySector('all')">All Sectors</button>
"""

    for sector in sectors:
        html_content += f'            <button class="sector-btn" onclick="filterBySector(\'{sector}\')">{sector}</button>\n'

    # Count breakouts and breakdowns for filter labels
    breakout_count = sum(1 for item in chart_data_list
                         if item['chart_data']['price'][-1] > item['chart_data']['band_high'][-1])
    breakdown_count = sum(1 for item in chart_data_list
                          if item['chart_data']['price'][-1] < item['chart_data']['band_low'][-1])

    # Serialize saved portfolios for embedding in JS
    portfolios_json = json.dumps(portfolios)

    html_content += f"""        </div>
        <p class="filter-label">Filter by Signal:</p>
        <div class="signal-filters">
            <button class="signal-btn active" data-signal="all" onclick="filterBySignal('all')">All</button>
            <button class="signal-btn" data-signal="breakout" onclick="filterBySignal('breakout')">Breakouts ({breakout_count})</button>
            <button class="signal-btn" data-signal="breakdown" onclick="filterBySignal('breakdown')">Breakdowns ({breakdown_count})</button>
        </div>
        <div class="portfolio-section">
            <p class="filter-label">Custom Portfolios (click to filter, edit to modify, drag to reorder):</p>
            <div class="portfolio-filters" id="portfolioFilters">
                <button class="create-portfolio-btn" onclick="openCreateModal()">+ New Portfolio</button>
            </div>
        </div>
    </header>

    <!-- Portfolio Create/Edit Modal -->
    <div class="portfolio-modal" id="portfolioModal">
        <div class="modal-content">
            <h3 id="modalTitle">Create Portfolio</h3>
            <label>Portfolio Name</label>
            <input type="text" id="portfolioNameInput" placeholder="e.g. My Top Picks">
            <label>Add Tickers</label>
            <input type="text" id="tickerAddInput" placeholder="Type ticker and press Enter..." onkeydown="handleTickerInput(event)">
            <p class="modal-hint" id="tickerHint">Any ticker is accepted. Tickers not in the chartbook will be fetched on the next run.</p>
            <div class="ticker-chips" id="tickerChips"></div>
            <div class="modal-actions">
                <button class="btn-cancel" onclick="closeModal()">Cancel</button>
                <button class="btn-save" onclick="savePortfolio()">Save</button>
            </div>
        </div>
    </div>

    <div class="charts-container" id="chartsContainer">
"""

    # Sort by ticker for initial display
    chart_data_sorted = sorted(chart_data_list, key=lambda x: x['ticker'])

    for i, item in enumerate(chart_data_sorted, 1):
        ticker = item['ticker']
        company_name = item['company_name']
        mcap = item['mcap']
        sector = item['sector']
        chart_json = json.dumps(item['chart_data']).replace("'", "&#39;")

        # Format market cap for display
        if mcap >= 1e12:
            mcap_str = f"${mcap/1e12:.2f}T"
        elif mcap >= 1e9:
            mcap_str = f"${mcap/1e9:.1f}B"
        elif mcap >= 1e6:
            mcap_str = f"${mcap/1e6:.0f}M"
        else:
            mcap_str = f"${mcap:,.0f}"

        # Determine breakout/breakdown signal from last data point
        cd = item['chart_data']
        last_price = cd['price'][-1] if cd['price'] else 0
        last_high = cd['band_high'][-1] if cd['band_high'] else 0
        last_low = cd['band_low'][-1] if cd['band_low'] else 0
        if last_price > last_high:
            signal = 'breakout'
        elif last_price < last_low:
            signal = 'breakdown'
        else:
            signal = 'neutral'

        html_content += f"""        <div class="chart-card" data-ticker="{ticker}" data-company="{company_name}" data-mcap="{mcap}" data-sector="{sector}" data-signal="{signal}">
            <div class="chart-header">
                <div>
                    <span class="ticker">{ticker}</span>
                    <span class="company-name">{company_name}</span>
                    <span class="market-cap">{mcap_str}</span>
                    <span class="sector-tag">{sector}</span>
                </div>
                <span class="chart-number">#{i} of {len(chart_data_list)}</span>
            </div>
            <div class="chart-container" id="chart-{ticker}" data-chart='{chart_json}'>
                <div class="chart-placeholder">
                    <div class="loading-spinner"></div>
                </div>
            </div>
            <a href="#top" class="back-to-top">↑ Back to Top</a>
        </div>
"""

    html_content += f"""    </div>
    <footer>Generated {timestamp} - Interactive Plotly Charts with Lazy Loading</footer>
    <script>
        let currentSort = 'ticker';
        let selectedSectors = [];
        let selectedSignal = null;
        let selectedPortfolio = null;
        let editingIndex = -1;
        let modalTickers = [];
        const renderedCharts = new Set();

        // Ordered array: [{{name: "...", tickers: ["...", ...]}}]
        let portfolios = [];

        // Portfolios embedded from custom_portfolios.json at generation time
        const embeddedPortfolios = {portfolios_json};

        // Build set of all tickers present in the chartbook
        const chartbookTickers = new Set();
        document.querySelectorAll('.chart-card').forEach(c => chartbookTickers.add(c.dataset.ticker.toUpperCase()));

        // --- Portfolio persistence ---
        function loadPortfolios() {{
            // Prefer localStorage; fall back to embedded from file
            try {{
                const saved = localStorage.getItem('chartbook_portfolios_v2');
                if (saved) {{
                    portfolios = JSON.parse(saved);
                }} else if (embeddedPortfolios.length > 0) {{
                    portfolios = JSON.parse(JSON.stringify(embeddedPortfolios));
                    savePortfoliosToStorage();
                }}
            }} catch(e) {{
                portfolios = [];
            }}
            renderPortfolioButtons();
        }}

        function savePortfoliosToStorage() {{
            localStorage.setItem('chartbook_portfolios_v2', JSON.stringify(portfolios));
        }}

        function findPortfolio(name) {{
            return portfolios.findIndex(p => p.name === name);
        }}

        // --- Render portfolio buttons ---
        let dragFromIdx = -1;

        function renderPortfolioButtons() {{
            const container = document.getElementById('portfolioFilters');
            container.innerHTML = '';

            portfolios.forEach((p, idx) => {{
                const btn = document.createElement('button');
                btn.className = 'portfolio-btn' + (selectedPortfolio === p.name ? ' active' : '');
                btn.draggable = true;
                btn.dataset.idx = idx;

                const label = document.createTextNode(p.name + ' (' + p.tickers.length + ') ');
                btn.appendChild(label);

                // Edit
                const edit = document.createElement('span');
                edit.className = 'edit-portfolio';
                edit.title = 'Edit portfolio';
                edit.textContent = '\u270E';
                edit.addEventListener('click', function(e) {{ e.stopPropagation(); openEditModal(idx); }});
                btn.appendChild(edit);

                // Delete
                const del = document.createElement('span');
                del.className = 'delete-portfolio';
                del.title = 'Delete portfolio';
                del.textContent = '\u2715';
                del.addEventListener('click', function(e) {{ e.stopPropagation(); deletePortfolio(idx); }});
                btn.appendChild(del);

                // Drag events
                btn.addEventListener('dragstart', function(e) {{
                    dragFromIdx = idx;
                    btn.classList.add('dragging');
                    e.dataTransfer.effectAllowed = 'move';
                }});
                btn.addEventListener('dragend', function() {{
                    btn.classList.remove('dragging');
                    dragFromIdx = -1;
                    container.querySelectorAll('.portfolio-btn').forEach(b => b.classList.remove('drag-over'));
                }});
                btn.addEventListener('dragover', function(e) {{
                    e.preventDefault();
                    e.dataTransfer.dropEffect = 'move';
                    container.querySelectorAll('.portfolio-btn').forEach(b => b.classList.remove('drag-over'));
                    if (parseInt(btn.dataset.idx) !== dragFromIdx) btn.classList.add('drag-over');
                }});
                btn.addEventListener('dragleave', function() {{
                    btn.classList.remove('drag-over');
                }});
                btn.addEventListener('drop', function(e) {{
                    e.preventDefault();
                    btn.classList.remove('drag-over');
                    const toIdx = parseInt(btn.dataset.idx);
                    if (dragFromIdx >= 0 && dragFromIdx !== toIdx) {{
                        const moved = portfolios.splice(dragFromIdx, 1)[0];
                        portfolios.splice(toIdx, 0, moved);
                        savePortfoliosToStorage();
                        renderPortfolioButtons();
                    }}
                }});

                btn.addEventListener('click', function() {{ filterByPortfolio(p.name); }});
                container.appendChild(btn);
            }});

            // + New Portfolio button
            const createBtn = document.createElement('button');
            createBtn.className = 'create-portfolio-btn';
            createBtn.textContent = '+ New Portfolio';
            createBtn.onclick = openCreateModal;
            container.appendChild(createBtn);

            // Export button (only show if portfolios exist)
            if (portfolios.length > 0) {{
                const exportBtn = document.createElement('button');
                exportBtn.className = 'export-portfolio-btn';
                exportBtn.textContent = '\u21E9 Export to File';
                exportBtn.title = 'Download custom_portfolios.json for next run';
                exportBtn.addEventListener('click', exportPortfolios);
                container.appendChild(exportBtn);
            }}
        }}

        // --- Filter ---
        function filterByPortfolio(name) {{
            if (selectedPortfolio === name) {{
                selectedPortfolio = null;
            }} else {{
                selectedPortfolio = name;
            }}
            renderPortfolioButtons();
            filterCharts();
        }}

        // --- Delete ---
        function deletePortfolio(idx) {{
            const name = portfolios[idx].name;
            if (!confirm('Delete portfolio "' + name + '"?')) return;
            portfolios.splice(idx, 1);
            if (selectedPortfolio === name) selectedPortfolio = null;
            savePortfoliosToStorage();
            renderPortfolioButtons();
            filterCharts();
        }}

        // --- Create modal ---
        function openCreateModal() {{
            editingIndex = -1;
            modalTickers = [];
            document.getElementById('modalTitle').textContent = 'Create Portfolio';
            document.getElementById('portfolioNameInput').value = '';
            document.getElementById('portfolioNameInput').disabled = false;
            document.getElementById('tickerAddInput').value = '';
            document.getElementById('tickerChips').innerHTML = '';
            document.getElementById('tickerHint').textContent = 'Any ticker is accepted. Tickers not in the chartbook will be fetched on the next run.';
            document.getElementById('tickerHint').className = 'modal-hint';
            document.getElementById('portfolioModal').classList.add('visible');
        }}

        // --- Edit modal ---
        function openEditModal(idx) {{
            editingIndex = idx;
            const p = portfolios[idx];
            modalTickers = [...p.tickers];
            document.getElementById('modalTitle').textContent = 'Edit Portfolio: ' + p.name;
            document.getElementById('portfolioNameInput').value = p.name;
            document.getElementById('portfolioNameInput').disabled = false;
            document.getElementById('tickerAddInput').value = '';
            document.getElementById('tickerHint').textContent = 'Any ticker is accepted. Tickers not in the chartbook will be fetched on the next run.';
            document.getElementById('tickerHint').className = 'modal-hint';
            renderModalChips();
            document.getElementById('portfolioModal').classList.add('visible');
        }}

        function closeModal() {{
            document.getElementById('portfolioModal').classList.remove('visible');
        }}

        // --- Ticker input (accepts any ticker) ---
        function handleTickerInput(e) {{
            if (e.key !== 'Enter') return;
            e.preventDefault();
            const input = document.getElementById('tickerAddInput');
            const ticker = input.value.trim().toUpperCase();
            input.value = '';
            if (!ticker) return;
            if (modalTickers.includes(ticker)) return;
            modalTickers.push(ticker);
            renderModalChips();
            // Warn if ticker is not in current chartbook
            if (!chartbookTickers.has(ticker)) {{
                const hint = document.getElementById('tickerHint');
                hint.textContent = ticker + ' is not in the current chartbook. It will be fetched on the next run.';
                hint.className = 'modal-hint warn';
            }}
        }}

        function removeModalTicker(ticker) {{
            modalTickers = modalTickers.filter(t => t !== ticker);
            renderModalChips();
        }}

        function renderModalChips() {{
            const container = document.getElementById('tickerChips');
            container.innerHTML = '';
            modalTickers.forEach(t => {{
                const chip = document.createElement('span');
                chip.className = 'ticker-chip';
                if (!chartbookTickers.has(t)) chip.style.borderColor = '#f59e0b';

                const tickerText = document.createTextNode(t + ' ');
                chip.appendChild(tickerText);

                const remove = document.createElement('span');
                remove.className = 'remove-ticker';
                remove.textContent = '\u2715';
                remove.addEventListener('click', function() {{ removeModalTicker(t); }});
                chip.appendChild(remove);

                container.appendChild(chip);
            }});
        }}

        // --- Save portfolio ---
        function savePortfolio() {{
            const name = document.getElementById('portfolioNameInput').value.trim();
            if (!name) {{
                document.getElementById('portfolioNameInput').placeholder = 'Name required';
                return;
            }}
            if (modalTickers.length === 0) {{
                alert('Add at least one ticker to the portfolio.');
                return;
            }}
            // Check for duplicate name (excluding current when editing)
            const dupIdx = findPortfolio(name);
            if (dupIdx !== -1 && dupIdx !== editingIndex) {{
                alert('A portfolio named "' + name + '" already exists.');
                return;
            }}
            if (editingIndex >= 0) {{
                portfolios[editingIndex] = {{ name: name, tickers: [...modalTickers] }};
            }} else {{
                portfolios.push({{ name: name, tickers: [...modalTickers] }});
            }}
            savePortfoliosToStorage();
            closeModal();
            renderPortfolioButtons();
            filterCharts();
        }}

        // --- Export portfolios to downloadable JSON file ---
        async function exportPortfolios() {{
            const data = JSON.stringify({{ portfolios: portfolios }}, null, 4);
            // Use File System Access API to let user pick save location (remembers last folder)
            if (window.showSaveFilePicker) {{
                try {{
                    const handle = await window.showSaveFilePicker({{
                        suggestedName: 'custom_portfolios.json',
                        types: [{{ description: 'JSON', accept: {{ 'application/json': ['.json'] }} }}]
                    }});
                    const writable = await handle.createWritable();
                    await writable.write(data);
                    await writable.close();
                    return;
                }} catch(e) {{
                    if (e.name === 'AbortError') return;
                }}
            }}
            // Fallback for browsers without File System Access API
            const blob = new Blob([data], {{ type: 'application/json' }});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'custom_portfolios.json';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }}

        // Close modal on background click
        document.getElementById('portfolioModal').addEventListener('click', function(e) {{
            if (e.target === this) closeModal();
        }});

        loadPortfolios();

        // Intersection Observer for lazy loading
        const observer = new IntersectionObserver((entries) => {{
            entries.forEach(entry => {{
                if (entry.isIntersecting) {{
                    const container = entry.target;
                    const chartId = container.id;

                    if (!renderedCharts.has(chartId)) {{
                        renderedCharts.add(chartId);
                        renderChart(container);
                    }}
                }}
            }});
        }}, {{
            rootMargin: '200px 0px',
            threshold: 0.01
        }});

        // Observe all chart containers
        document.querySelectorAll('.chart-container').forEach(container => {{
            observer.observe(container);
        }});

        function renderChart(container) {{
            // Clear the loading placeholder
            container.innerHTML = '';

            const data = JSON.parse(container.dataset.chart);

            // Create custom hover text with all values for each point
            const customText = data.date_labels.map((date, i) =>
                '<b>' + date + '</b><br>' +
                'Price: $' + data.price[i].toFixed(2) + '<br>' +
                'Lower Band: $' + data.band_low[i].toFixed(2) + '<br>' +
                'Upper Band: $' + data.band_high[i].toFixed(2)
            );

            const traces = [
                {{
                    x: data.dates,
                    y: data.price,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Price',
                    line: {{ color: '#dc2626', width: 2 }},
                    text: customText,
                    hovertemplate: '%{{text}}<extra></extra>'
                }},
                {{
                    x: data.dates,
                    y: data.band_low,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Lower Band',
                    line: {{ color: '#2563eb', width: 1.5 }},
                    text: customText,
                    hovertemplate: '%{{text}}<extra></extra>'
                }},
                {{
                    x: data.dates,
                    y: data.band_high,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Upper Band',
                    line: {{ color: '#2563eb', width: 1.5 }},
                    text: customText,
                    hovertemplate: '%{{text}}<extra></extra>'
                }}
            ];

            const layout = {{
                title: {{
                    text: data.company_name,
                    font: {{ size: 16, color: '#1f2937', family: 'system-ui, sans-serif', weight: 'bold' }}
                }},
                xaxis: {{
                    showgrid: true,
                    gridcolor: '#e5e7eb',
                    tickmode: 'array',
                    tickvals: data.tick_vals,
                    ticktext: data.tick_text,
                    tickangle: -90,
                    tickfont: {{ size: 10 }},
                    type: 'date'
                }},
                yaxis: {{
                    showgrid: true,
                    gridcolor: '#e5e7eb',
                    tickprefix: '$',
                    tickformat: ',.0f',
                    tickfont: {{ size: 11 }}
                }},
                margin: {{ l: 70, r: 30, t: 50, b: 100 }},
                paper_bgcolor: '#ffffff',
                plot_bgcolor: '#ffffff',
                showlegend: false,
                hovermode: 'closest'
            }};

            const config = {{
                responsive: true,
                displayModeBar: true,
                modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
                displaylogo: false
            }};

            Plotly.newPlot(container.id, traces, layout, config);
        }}

        function filterCharts() {{
            const input = document.getElementById('searchInput').value.toLowerCase();
            const cards = document.querySelectorAll('.chart-card');
            const activeP = selectedPortfolio ? portfolios.find(p => p.name === selectedPortfolio) : null;
            const portfolioTickers = activeP ? new Set(activeP.tickers.map(t => t.toUpperCase())) : null;
            cards.forEach(card => {{
                const ticker = card.dataset.ticker.toLowerCase();
                const company = card.dataset.company.toLowerCase();
                const sector = card.dataset.sector;
                const signal = card.dataset.signal;
                const matchesSearch = ticker.includes(input) || company.includes(input);
                const matchesSector = selectedSectors.length === 0 || selectedSectors.includes(sector);
                const matchesSignal = !selectedSignal || selectedSignal === signal;
                const matchesPortfolio = !portfolioTickers || portfolioTickers.has(card.dataset.ticker.toUpperCase());
                if (matchesSearch && matchesSector && matchesSignal && matchesPortfolio) {{
                    card.classList.remove('hidden');
                }} else {{
                    card.classList.add('hidden');
                }}
            }});
            updateVisibleNumbers();
        }}

        function filterBySector(sector) {{
            if (sector === 'all') {{
                selectedSectors = [];
            }} else {{
                const index = selectedSectors.indexOf(sector);
                if (index > -1) {{
                    selectedSectors.splice(index, 1);
                }} else {{
                    selectedSectors.push(sector);
                }}
            }}

            const sectorBtns = document.querySelectorAll('.sector-btn');
            sectorBtns.forEach(btn => {{
                btn.classList.remove('active');
                if (btn.textContent === 'All Sectors' && selectedSectors.length === 0) {{
                    btn.classList.add('active');
                }} else if (selectedSectors.includes(btn.textContent)) {{
                    btn.classList.add('active');
                }}
            }});

            filterCharts();
        }}

        function filterBySignal(signal) {{
            if (signal === 'all') {{
                selectedSignal = null;
            }} else {{
                selectedSignal = selectedSignal === signal ? null : signal;
            }}

            const signalBtns = document.querySelectorAll('.signal-btn');
            signalBtns.forEach(btn => {{
                btn.classList.remove('active');
                if (btn.dataset.signal === 'all' && !selectedSignal) {{
                    btn.classList.add('active');
                }} else if (btn.dataset.signal === selectedSignal) {{
                    btn.classList.add('active');
                }}
            }});

            filterCharts();
        }}

        function updateVisibleNumbers() {{
            const cards = Array.from(document.querySelectorAll('.chart-card:not(.hidden)'));
            cards.forEach((card, index) => {{
                card.querySelector('.chart-number').textContent = '#' + (index + 1) + ' of ' + cards.length;
            }});
        }}

        function sortByTicker() {{
            if (currentSort === 'ticker') return;
            currentSort = 'ticker';
            updateSortButtonStates();

            const container = document.getElementById('chartsContainer');
            const cards = Array.from(container.querySelectorAll('.chart-card'));

            cards.forEach(card => card.remove());
            cards.sort((a, b) => a.dataset.ticker.localeCompare(b.dataset.ticker));
            cards.forEach(card => container.appendChild(card));

            filterCharts();
        }}

        function sortByMarketCap() {{
            if (currentSort === 'mcap') return;
            currentSort = 'mcap';
            updateSortButtonStates();

            const container = document.getElementById('chartsContainer');
            const cards = Array.from(container.querySelectorAll('.chart-card'));

            cards.forEach(card => card.remove());
            cards.sort((a, b) => parseFloat(b.dataset.mcap) - parseFloat(a.dataset.mcap));
            cards.forEach(card => container.appendChild(card));

            filterCharts();
        }}

        function updateSortButtonStates() {{
            const buttons = document.querySelectorAll('.sort-btn');
            buttons.forEach(btn => btn.classList.remove('active'));
            if (currentSort === 'ticker') {{
                buttons[0].classList.add('active');
            }} else {{
                buttons[1].classList.add('active');
            }}
        }}
    </script>
</body>
</html>"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"\nChartbook saved to: {output_file}")


def main():
    print("=" * 60)
    print("Direct Bloomberg Chartbook Generator - S&P 500")
    print("Interactive Plotly Charts with Lazy Loading")
    print("=" * 60)

    # Check Bloomberg availability
    if not check_bloomberg_available():
        print("\nERROR: Bloomberg API (blpapi) is not available.")
        print("Please install it first:")
        print("  python -m pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi")
        sys.exit(1)

    print("\nBloomberg API (blpapi) is available!")
    print("Make sure Bloomberg Terminal is running.")

    # Configuration
    TOP_N = None  # Set to 10, 50, etc. to limit, or None for all
    SORT_BY_MCAP = True  # Sort by market cap (largest first)

    # Load custom portfolios
    print("\n" + "-" * 60)
    print("Loading custom portfolios...")
    saved_portfolios, custom_ticker_set = load_custom_portfolios()

    # Start timing
    total_start = time.time()

    # Initialize Bloomberg session (ONCE for all requests)
    print("\n" + "-" * 60)
    print("Step 1: Starting Bloomberg session...")
    bbg = BloombergSession()
    bbg.start()
    print("  Bloomberg session started.")

    try:
        # Fetch S&P 500 constituents
        print("\nStep 2: Fetching S&P 500 constituents...")
        tickers = bbg.get_sp500_constituents()
        sp500_set = set(tickers)
        print(f"  Retrieved {len(tickers)} constituents")

        # Merge in custom portfolio tickers not already in S&P 500
        extra_tickers = custom_ticker_set - sp500_set
        if extra_tickers:
            tickers = tickers + sorted(extra_tickers)
            print(f"  Added {len(extra_tickers)} custom tickers: {', '.join(sorted(extra_tickers))}")

        # Batch fetch names, sectors, AND market caps in one combined request
        print("\nStep 3: Fetching metadata (names + sectors + market caps, batched)...")
        metadata_start = time.time()
        company_names, gics_sectors, market_caps = bbg.get_metadata_batch(tickers)
        print(f"  Got {len(company_names)} names, {len(gics_sectors)} sectors, {len(market_caps)} market caps in {time.time() - metadata_start:.1f}s")

        # Sort/filter tickers now that we have market caps
        if SORT_BY_MCAP or TOP_N:
            tickers = sorted(market_caps.keys(), key=lambda t: market_caps.get(t, 0), reverse=True)
            if TOP_N:
                tickers = tickers[:TOP_N]
        else:
            tickers = sorted(tickers)

        if TOP_N:
            print(f"  Using top {len(tickers)} constituents by market cap")
        else:
            print(f"  Using {len(tickers)} constituents" + (" (sorted by market cap)" if SORT_BY_MCAP else ""))

        # Batch fetch historical data
        print("\nStep 4: Fetching historical data (batched)...")
        data_start = time.time()
        historical_data = bbg.get_historical_data_batch(tickers, years=YEARS_FOR_PERCENTILES)
        print(f"  Got {len(historical_data)} datasets in {time.time() - data_start:.1f}s")

    finally:
        # Always stop the session
        print("\nStep 5: Stopping Bloomberg session...")
        bbg.stop()
        print("  Bloomberg session stopped.")

    # Generate chart data in parallel
    print("\nStep 6: Generating chart data (parallel)...")
    chart_start = time.time()

    # Build list of tasks for parallel processing
    tasks = []
    skipped = []
    for ticker in tickers:
        if ticker not in historical_data:
            skipped.append(ticker)
            continue
        tasks.append((ticker, historical_data[ticker], company_names.get(ticker, ticker), YEARS_TO_PLOT))

    chart_data_list = []
    failed = []

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {
            executor.submit(generate_chart_data, t, df, name, yrs): t
            for t, df, name, yrs in tasks
        }
        done_count = 0
        for future in as_completed(futures):
            ticker = futures[future]
            chart_json, error = future.result()
            done_count += 1
            if error:
                failed.append(ticker)
            else:
                chart_data_list.append({
                    'ticker': ticker,
                    'company_name': company_names.get(ticker, ticker),
                    'mcap': market_caps.get(ticker, 0),
                    'sector': gics_sectors.get(ticker, 'Unknown'),
                    'chart_data': chart_json
                })
            if done_count % 50 == 0 or done_count == len(tasks):
                print(f"  Progress: {done_count}/{len(tasks)} tickers processed...")

    print(f"  Chart data generated in {time.time() - chart_start:.1f}s")

    # Summary
    print("\n" + "-" * 60)
    total_time = time.time() - total_start
    print(f"Completed: {len(chart_data_list)}/{len(tickers)} charts generated")

    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed[:10])}" + ("..." if len(failed) > 10 else ""))
    if skipped:
        print(f"Skipped ({len(skipped)}): {', '.join(skipped[:10])}" + ("..." if len(skipped) > 10 else ""))

    # Generate HTML chartbook
    if chart_data_list:
        generate_html_chartbook(chart_data_list, HTML_OUTPUT, portfolios=saved_portfolios)

    print(f"\nTotal time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"Open {HTML_OUTPUT} in a browser to view the chartbook.")


if __name__ == "__main__":
    main()
