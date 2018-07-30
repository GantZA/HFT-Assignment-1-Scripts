using JuliaDB
using CSV
using OnlineStats

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

'''
Cleaning intraday
'''

ask_files = glob("*.csv",string(dir_intday, "/asks"))
bid_files = glob("*.csv",string(dir_intday, "/bids"))
function clean_intday_trades(path)
    trade_files = glob("*.csv",string(path, "/trades"))
    clean_trades = loadtable(trade_files[1])
    #for each in trade_files[2:end]
    #    clean_trades = vcat(clean_trades,loadtable(each))
    #end
    return clean_trades
end

a = clean_intday_trades(dir_intday)
function clean_intday_quote(path)
    ask_files = glob("*.csv",string(path, "/asks"))
    clean_asks = keep_non_auction(loadtable(ask_files[1]),10)
    for i in 2:6
        clean_asks =vcat(clean_asks, keep_non_auction(loadtable(ask_files[i]), 10))
    end
    return clean_asks
end

clean_ask = clean_intday_quote(dir_intday)

function keep_non_auction(data, noise_limit=0)
    return filter(i -> (((Dates.hour(i.times) >= 9) && (Dates.minute(i.times) >= noise_limit)) && ((Dates.hour(i.times) < 16) || ((Dates.hour(i.times) == 16) && (Dates.minute(i.times) < 50)))), data)
end


raw_trades = loadtable(files_intday[21])
for i in 22:30
    raw_trades = vcat(test, loadtable(files_intday[i]))
end

clean_trades = remove_non_valid_times(raw_trades[1])
for each in raw_trades
    clean_trades = vcat(clean_trades, remove_non_valid_times(each))
end

function tick_rule(trades)
    ####
    '''
        First we collapse continuous sequences of trades into one aggregate trade
    '''
    ####
end

function qoute_rule(trades)
    '''
        We consider a price change to have been initiated by a buy order if the
        price of a trade is higher than the prevailing mid quote and vice versa
        for sell orders.
    '''
end

function volume_price(x)
    return x[1].value .* x[1].size / sum(x[1].size)
end

function trade_compact(trades)
    agg_volume = groupby(sum, trades, :times, select=:size)
    vol_price = groupby(volume_price, trades, :times)
    new_table = join(agg_volume, vol_price)
    return new_table
end
trade_compact(a)


function quote_compact(quotes)
    '''
    For sequences of quotes with the same date-time stamps the quote most
    recent in sequence quote is retained and the remainder removed. This
    sometimes requires some interrogation of the sequences of quotes with
    additional rules.
    '''
end


#Construct a dataset for each share












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
