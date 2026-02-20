import numpy
import talib
from numpy import genfromtxt

my_data = genfromtxt('15minutes.csv', delimiter= ',')

print(my_data)

# 5번째 데이터가 체결가, 즉 종가이다
close = my_data[:,4]

print(close)

# close = numpy.random.random(100)

# print(close)

# moving_average = talib.SMA(close, timeperiod = 10)

# print(moving_average)

# rsi = talib.RSI(close)

# print('rsi')
# print(rsi)