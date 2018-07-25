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

function clean_intday(path)
    files = glob("*.csv",path)

end

function remove_non_valid_times(data)
    return filter(i -> ((Dates.hour(i.times) >= 8) && ((Dates.hour(i.times) < 17) || ((Dates.hour(i.times) == 16) && (Dates.minute(i.times) <= 50)))), data)
end

raw_trades = loadtable(files_intday[21])
for i in 22:30
    raw_trades = vcat(test, loadtable(files_intday[i]))
end

clean_trades = remove_non_valid_times(raw_trades[1])
for each in raw_trades
    clean_trades = vcat(clean_trades, remove_non_valid_times(each))
end

clean_trades















raw_trades





files_intday[21:30]




files_intday[21]
test1 = loadtable(files_intday[21])
test2 = loadtable(files_intday[22])
test = test1
test = vcat(test, test2)

test = (test1, test2)
typeof(test)
test1 = filter(i -> ((Dates.hour(i.times) >= 8) && ((Dates.hour(i.times) < 17) || ((Dates.hour(i.times) == 16) && (Dates.minute(i.times) <= 50)))), test)

test[1]
test[end]
dump(test[1].times)
Dates.hour(test[20000].times)
test2 = filter(i -> (Date(i.times) == Date("2018-01-23")), test1)

Date(test1[1].times) == Date("2018-01-23")

test3 = filter(i -> (Dates.hour(i.times) == 16), test)
files_intday

test
