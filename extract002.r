library(Rblpapi)
con <- blpConnect() 

#getBars
security = c("CPI SJ Equity", "SBK SJ Equity", "SOL SJ Equity","CLS SJ Equity", "FSR SJ Equity", "SLM SJ Equity","MTN SJ Equity", 
             "NED SJ Equity", "INL SJ Equity", "NPN SJ Equity")


##################################################### Daily Sampled OHLCV
barInt = 540
StartTime = as.POSIXct("2017-06-1 08:00:00")

daily_sampled = list()
for (i in 1:10){
  daily_sampled[[i]] = getBars(security = security[i], barInterval = 540, startTime = StartTime, con = con)
  file_name = paste("F:/data/daily_sampled/", substr(security[i],1,3), ".csv", sep = "")
  write.csv(daily_sampled[[i]], file_name, row.names = F)
}

################################################### Intraday   
StartTime = as.POSIXct("2018-01-23 08:00:00")

for (i in 1:10){
  file_name = paste("F:/data/intraday/trade_", substr(security[i],1,3), ".csv", sep = "")
  temp = getTicks(security = security[i], eventType = c("TRADE"), startTime = StartTime, con = con)
  write.csv(temp, file_name, row.names = F)
}

for (i in 1:10){
  file_name = paste("F:/data/intraday/ask_", substr(security[i],1,3), ".csv", sep = "")
  temp = getTicks(security = security[i], eventType = c("ASK"), startTime = StartTime, con = con)
  write.csv(temp, file_name, row.names = F)
}

for (i in 1:10){
  file_name = paste("F:/data/intraday/bid_", substr(security[i],1,3), ".csv", sep = "")
  temp = getTicks(security = security[i], eventType = c("BID"), startTime = StartTime, con = con)
  write.csv(temp, file_name, row.names = F)
}

i = 2
file_name = paste("F:/data/intraday/trade_", substr(security[i],1,3), ".csv", sep = "")
temp = getTicks(security = security[i], eventType = c("TRADE"), startTime = StartTime, con = con)
write.csv(temp, file_name, row.names = F)



file_name = paste("F:/data/intraday/ask_", substr(security[i],1,3), ".csv", sep = "")
temp = getTicks(security = security[i], eventType = c("ASK"), startTime = StartTime, con = con)
write.csv(temp, file_name, row.names = F)



file_name = paste("F:/data/intraday/bid_", substr(security[i],1,3), ".csv", sep = "")
temp = getTicks(security = security[i], eventType = c("BID"), startTime = StartTime, con = con)
write.csv(temp, file_name, row.names = F)


