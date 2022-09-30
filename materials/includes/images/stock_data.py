import pickle
import yfinance as yf

tsla = yf.Ticker('TSLA')

print(tsla.recommendations["To Grade"].value_counts().keys()[0])