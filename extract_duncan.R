library(Rblpapi)
library(lubridate)
con <- blpConnect() 
################################

#Before Running make sure your directory and security variables are correct!   #######
dir = "F:/data/"

#################################
security = c("APN SJ EQUITY", "CPI SJ EQUITY", "BIL SJ EQUITY","INP SJ EQUITY", "AGL SJ EQUITY", "TFG SJ EQUITY", "BTI SJ EQUITY" , "MTN SJ EQUITY",
             "NPN SJ EQUITY","SHP SJ EQUITY")

##################################################### Daily Sampled OHLCV
barInt = 540
StartTime = as.POSIXct("2017-06-1 08:00:00")    #### Doesnt seem to matter what date as long as it is at least more than 6 months ago

daily_sampled = list()
for (i in 1:10){
  daily_sampled[[i]] = getBars(security = security[i], barInterval = 540, startTime = StartTime, con = con)
  file_name = paste(dir,"daily_sampled/", substr(security[i],1,3), ".csv", sep = "")
  write.csv(daily_sampled[[i]], file_name, row.names = F)
}

################################################### Intraday   
StartTime = as.POSIXct("2018-01-28 05:00:00")

for (i in 1:10){
  file_name = paste(dir, "intraday/trade_", substr(security[i],1,3), ".csv", sep = "")
  temp = getTicks(security = security[i], eventType = c("TRADE"), startTime = StartTime, con = con)
  write.csv(temp, file_name, row.names = F)
}

for (i in 1:10){
  StartTime = as.POSIXct("2018-01-28 05:00:00")
  EndTime = StartTime
  month(EndTime) = month(StartTime) + 1
  for (k in 1:6){
    file_name = paste(dir,"intraday/ask_","00", k,"_" , substr(security[i],1,3), ".csv", sep = "")
    temp = getTicks(security = security[i], eventType = c("ASK"), startTime = StartTime, endTime = EndTime,  
                    con = con)
    write.csv(temp, file_name, row.names = F)
    StartTime = EndTime
    month(EndTime) = month(EndTime) + 1
  }
}

for (i in 1:10){
  StartTime = as.POSIXct("2018-01-28 05:00:00")
  EndTime = StartTime
  month(EndTime) = month(StartTime) + 1
  for (k in 1:6){
    file_name = paste(dir, "intraday/bid_","00", k,"_" , substr(security[i],1,3), ".csv", sep = "")
    temp = getTicks(security = security[i], eventType = c("BID"), startTime = StartTime, endTime = EndTime,  
                    con = con)
    write.csv(temp, file_name, row.names = F)
    StartTime = EndTime
    month(EndTime) = month(EndTime) + 1
  }
}


