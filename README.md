#  play-spark
##  Run Bitcoin data application 

git clone 

cd play-spark
sbt clean run 


### get to get bitcoin price history for last week 
	/v1/pricehistory?period=week
e.g http://localhost:9000/v1/pricehistory?period=week

### get to get bitcoin price history for last week with moving average 2 days
	/v1/pricehistory?period=week&rollingavg=2
e.g http://localhost:9000/v1/pricehistory?period=week&rollingavg=2


### get to get bitcoin price history for last month 
	/v1/pricehistory?period=month
e.g http://localhost:9000/v1/pricehistory?period=month

### get to get bitcoin price history for last month with moving average 2 days
	/v1/pricehistory?period=month&rollingavg=2
e.g http://localhost:9000/v1/pricehistory?period=month&rollingavg=2

### get to get bitcoin price history for specific period 
	/v1/pricehistoryperiod?start=2018-06-01&end=2018-06-06
http://localhost:9000/v1/pricehistoryperiod?start=2018-06-01&end=2018-06-06

### get to get bitcoin price history for specific period with moving average 2 days
	/v1/pricehistoryperiod?start=2018-06-01&end=2018-06-06&rollingavg=2
http://localhost:9000/v1/pricehistoryperiod?start=2018-06-01&end=2018-06-06&rollingavg=2

### get pedected price for x days
	/v1/priceforecast?days=15
http://localhost:9000/v1/priceforecast?days=15

### get pedected price for x days with moving average
/v1/priceforecast?days=15&rollingavg=3
http://localhost:9000/v1/priceforecast?days=15&rollingavg=3


### To display data with graphical report
	/display/pricehistory?period=week&rollingavg=2
http://localhost:9000/display/pricehistory?period=week&rollingavg=2
