import os
import time
import logging
import pandas as pd
from datetime import datetime
from binance.client import Client
import requests

# Binance API setup
API_KEY = "xxx"
API_SECRET = "xxxxx"
client = Client(API_KEY, API_SECRET)

# CSV file for saving data
CSV_FILE = "order_book_predictions.csv"
INTERVAL = 10  # Fetch data every 10 seconds
CHECK_ENTRIES = 5  # Check last 5 entries for trend detection
WHALER_THRESHOLD = 10  # BTC volume to be considered a whale

# Telegram Notification Setup
TELEGRAM_BOT_TOKEN = "7634717158:AAHMMksZXje9CEF4qEMn3Vgge5F_qNs6sHg"
TELEGRAM_CHAT_ID = "5463783915"
TELEGRAM_GROUP_ID = "-4695344604"

# Logging setup
logging.basicConfig(
    filename="order_book_predictions.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize CSV file
def initialize_csv(file_name):
    if not os.path.exists(file_name):
        df = pd.DataFrame(columns=[
            "timestamp", "total_bid_volume", "total_ask_volume",
            "largest_buy_wall_price", "largest_buy_wall_volume",
            "largest_sell_wall_price", "largest_sell_wall_volume",
            "spread", "prediction", "consecutive_wall_trend", "whale_alert"
        ])
        df.to_csv(file_name, index=False)

# Fetch order book data
def fetch_order_book(symbol="BTCUSDT", limit=100):
    try:
        return client.futures_order_book(symbol=symbol, limit=limit)
    except Exception as e:
        logging.error(f"Error fetching order book: {e}")
        return None

# Send Telegram Notification
def send_telegram_notification(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_GROUP_ID, "text": message, "parse_mode": "Markdown"}
        response = requests.post(url, json=payload)
        print(response.json())

        if response.status_code == 200:
            logging.info(f"Telegram Alert Sent: {message}")
        else:
            logging.error(f"Failed to send Telegram message: {response.text}")

    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")

# Function to send color-coded whale alerts
def send_whale_alert(volume, price, order_type):
    """Send color-coded whale alerts based on BTC volume."""
    
    # Define color coding based on volume range
    if 10 <= volume < 50:
        color = "⚫" if order_type == "BUY" else "⚪"
        level = "Small Whale"
    elif 50 <= volume < 100:
        color = "🟣" if order_type == "BUY" else "🟡"  # Purple for Buy, Yellow for Sell
        level = "Medium Whale"
    elif 100 <= volume < 150:
        color = "🔵" if order_type == "BUY" else "🟠"  # Blue for Buy, Orange for Sell
        level = "Large Whale"
    elif 150 <= volume < 200:
        color = "🟢" if order_type == "BUY" else "🔴"  # Green for Buy, Red for Sell
        level = "Mega Whale"
    else:
        return  # Ignore if volume is below 10

    # Format the message
    message = f"{color} *{level} Alert!*\n" \
              f"🐳 Large {order_type} Order Detected:\n" \
              f"🔼 {volume} BTC at {price}"

    # Send notification
    send_telegram_notification(message)

# Analyze order book
def analyze_order_book(order_book):
    try:
        bids = order_book["bids"]
        asks = order_book["asks"]

        total_bid_volume = sum([float(b[1]) for b in bids])
        total_ask_volume = sum([float(a[1]) for a in asks])
        largest_buy_wall = max(bids, key=lambda b: float(b[1]))
        largest_sell_wall = max(asks, key=lambda a: float(a[1]))
        spread = float(asks[0][0]) - float(bids[0][0])

        # Extract buy & sell volume and price
        buy_volume = float(largest_buy_wall[1])
        sell_volume = float(largest_sell_wall[1])
        buy_price = largest_buy_wall[0]
        sell_price = largest_sell_wall[0]

        # Send whale alerts based on volume thresholds
        if buy_volume >= WHALER_THRESHOLD:
            send_whale_alert(buy_volume, buy_price, "BUY")

        if sell_volume >= WHALER_THRESHOLD:
            send_whale_alert(sell_volume, sell_price, "SELL")

        return {
            "total_bid_volume": total_bid_volume,
            "total_ask_volume": total_ask_volume,
            "largest_buy_wall": (float(largest_buy_wall[0]), float(largest_buy_wall[1])),
            "largest_sell_wall": (float(largest_sell_wall[0]), float(largest_sell_wall[1])),
            "spread": spread
        }
    except Exception as e:
        logging.error(f"Error analyzing order book: {e}")
        return None

# Predict market sentiment
def predict_sentiment(analysis):
    if analysis["total_bid_volume"] > analysis["total_ask_volume"]:
        return "Bullish"
    elif analysis["total_bid_volume"] < analysis["total_ask_volume"]:
        return "Bearish"
    else:
        return "Neutral"

# Save data to CSV
def save_to_csv(data, file_name=CSV_FILE):
    try:
        df = pd.DataFrame([data])
        with open(file_name, mode="a", newline="") as f:
            df.to_csv(f, index=False, header=False)
            f.flush()  # Ensure data is written immediately
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")

# Main loop
def main():
    initialize_csv(CSV_FILE)
    while True:
        try:
            order_book = fetch_order_book()
            if order_book:
                analysis = analyze_order_book(order_book)
                if analysis:
                    prediction = predict_sentiment(analysis)

                    # ✅ Define timestamp before printing
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    data = {
                        "timestamp": timestamp,
                        "total_bid_volume": analysis["total_bid_volume"],
                        "total_ask_volume": analysis["total_ask_volume"],
                        "largest_buy_wall_price": analysis["largest_buy_wall"][0],
                        "largest_buy_wall_volume": analysis["largest_buy_wall"][1],
                        "largest_sell_wall_price": analysis["largest_sell_wall"][0],
                        "largest_sell_wall_volume": analysis["largest_sell_wall"][1],
                        "spread": analysis["spread"],
                        "prediction": prediction,
                        "consecutive_wall_trend": "",
                        "whale_alert": analysis["whale_alert"]
                    }
                    
                    # Save data to CSV
                    save_to_csv(data)

                    # ✅ Print in command prompt
                    print(f"[{timestamp}] Buy: {analysis['largest_buy_wall'][1]} BTC | "
                          f"Sell: {analysis['largest_sell_wall'][1]} BTC | Prediction: {prediction}")

                    # Print Whale Alert if detected
                    if analysis["whale_alert"]:
                        print(f"🚨 {analysis['whale_alert']}")

                    logging.info(f"Data saved: {data}")

        except Exception as e:
            logging.error(f"Error in main loop: {e}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    while True:
        main()
        time.sleep(INTERVAL)
