using Temporal
using JuliaDB
using CSV

dir_ds = "/Documents/UCT 2018/HFT/Assignment 1/data/daily_sampled"
dir_intday = "/Documents/UCT 2018/HFT/Assignment 1/data/intraday"
files_ds = glob("*.csv",dir_ds)
files_intday = glob("*.csv",dir_intday)

'''
Cleaning intraday
'''

function clean_intday(path, )

end

function keep_

files_intday[21:30]
files_intday[21]
test = loadtable(files_intday[21])

test1 = filter(i -> ((Dates.hour(i.times) >= 8) && ((Dates.hour(i.times) < 17) || ((Dates.hour(i.times) == 16) && (Dates.minute(i.times) <= 50)))), test)

test[1]
test[end]
dump(test[1].times)
Dates.hour(test[20000].times)
test2 = filter(i -> (Date(i.times) == Date("2018-01-23")), test1)

Date(test1[1].times) == Date("2018-01-23")

test3 = filter(i -> (Dates.hour(i.times) == 16), test)
