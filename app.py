from flask import Flask, render_template
import csv
from binance.client import Client
import config

client = Client(config.API_KEY, config.API_SECRET)

app = Flask(__name__)

@app.route('/')
def index():
    title = 'CoinView'
    print("API_KEY loaded?", bool(config.API_KEY), "len=", 0 if not config.API_KEY else len(config.API_KEY))
    print("API_SECRET loaded?", bool(config.API_SECRET), "len=", 0 if not config.API_SECRET else len(config.API_SECRET))
    

    return render_template('index.html', title= title)

@app.route('/buy')
def buy():
    return 'buy'

@app.route('/sell')
def sell():
    return 'sell'

@app.route('/settings')
def settings():
    return 'settings'