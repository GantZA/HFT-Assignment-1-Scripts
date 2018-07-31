using JuliaDB
using CSV
using OnlineStats
using JuliaDBMeta

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

trade_files = glob("*.csv",string(dir_intday, "/trades"))
ask_files = glob("*.csv",string(dir_intday, "/asks"))
bid_files = glob("*.csv",string(dir_intday, "/bids"))

function keep_non_auction(data, noise_limit=0)
    return filter(i -> ((Dates.hour(i.times) > 9 && Dates.hour(i.times) < 16) || (Dates.hour(i.times) == 9 && Dates.minute(i.times) >= noise_limit) || (Dates.hour(i.times) == 16 && Dates.minute(i.times) < 50)), data)
end

function trade_compact(trade_path)
    trades = loadtable(trade_path)
    agg_vol_price = groupby(
        @NT(
            aggregate_volume = :size => x -> sum(x),
            volume_weighted_price = (:size, :value) => x -> sum(map(i-> i.value * i.size, table(x)))/sum(map(i -> i.size, table(x)))
        ),
        trades,
        :times)
    new_table = keep_non_auction(agg_vol_price,10)
    return new_table
end

function quote_compact(quote_path)
    quotes = loadtable(quote_path)
    most_recent = groupby(
        @NT(
            size = :size => x -> x[end],
            value = :value => x -> x[end]
        ),
        quotes,
        :times)
    return keep_non_auction(most_recent,10)
end



ask_files[37]
function clean_intday_trades(path)
    trade_files = glob("*.csv",string(path, "/trades"))
    return trade_compact.(trade_files)
end

@time all_trades = clean_intday_trades(dir_intday)
# 116.034881 seconds (467.46 M allocations: 20.059 GiB, 7.11% gc time)

@time trade_compact.(trade_files)





NPN_best_ask_prices = quote_compact(ask_files[37])
NPN_best_bid_prices = quote_compact(bid_files[37])
NPN_trades = trade_compact(trade_files[7])

NPN_trades[1:5]







NPN_best_ask_prices[1:5]








NPN_best_bid_prices[1:5]
