using JuliaDB
using CSV
using Missings


dir_ds = "/Documents/UCT 2018/HFT/Assignment 1/data/daily_sampled"
dir_intday = "/Documents/UCT 2018/HFT/Assignment 1/data/intraday"

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



ask_files
function clean_intday_trades(path)
    trade_files = glob("*.csv",string(path, "/trades"))
    return trade_compact.(trade_files)
end

@time all_trades = clean_intday_trades(dir_intday)
# 116.034881 seconds (467.46 M allocations: 20.059 GiB, 7.11% gc time)

@time trade_compact.(trade_files)





NPN_best_ask_prices = quote_compact(ask_files[37])
NPN_best_ask_prices = renamecol(NPN_best_ask_prices, :size, :ask_size)
NPN_best_ask_prices = renamecol(NPN_best_ask_prices, :value, :ask_value)

NPN_best_bid_prices = quote_compact(bid_files[37])
NPN_best_bid_prices = renamecol(NPN_best_bid_prices, :size, :bid_size)
NPN_best_bid_prices = renamecol(NPN_best_bid_prices, :value, :bid_value)

NPN_trades = trade_compact(trade_files[7])


NPN_quotes = join(NPN_best_ask_prices, NPN_best_bid_prices, how=:outer)

mid_price = map(i-> (i.ask_value-i.bid_value)/2, NPN_quotes)

best_ask = NPN_quotes[1].ask_value.value
best_bid = NPN_quotes[1].bid_value.value
mid_price = Vector(length(NPN_quotes))
mid_price[1] = (best_ask - best_bid)/2
for i in 2:length(NPN_quotes)
    if NPN_quotes[i].ask_value.value > 0
        best_ask = NPN_quotes[i].ask_value.value
    end
    if NPN_quotes[i].bid_value.value > 0
        best_bid = NPN_quotes[i].bid_value.value
    end
    mid_price[i] = (best_ask - best_bid)/2
end

NPN_quotes[3].ask_value.value > 0
best_ask = NPN_quotes[1].ask_value.value

i = 1
table(mid_price)






NPN_best_bid_prices[1:5]
