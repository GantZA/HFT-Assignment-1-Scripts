using JuliaDB
using CSV


dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

trade_files = glob("*.csv",string(dir_intday, "/trades"))
ask_files = glob("*.csv",string(dir_intday, "/asks"))
bid_files = glob("*.csv",string(dir_intday, "/bids"))

function keep_non_auction(data, noise_limit=0)
    return filter(i -> ((Dates.hour(i.times) > 9 && Dates.hour(i.times) < 16) || (Dates.hour(i.times) == 9 && Dates.minute(i.times) >= noise_limit) || (Dates.hour(i.times) == 16 && Dates.minute(i.times) < 50)), data)
end

function remove_annomolies(data)
    return filter(i -> i.size > 0, data)
end

function get_tickers(path)
    return path[end-2:end]
end

tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))


function trade_compact(trade_path)
    trades =  remove_annomolies(keep_non_auction(loadtable(trade_path),10))
    agg_vol_price = groupby(
        @NT(
            aggregate_volume = :size => x -> sum(x),
            volume_weighted_price = (:size, :value) => x -> sum(map(i-> i.value * i.size, table(x)))/sum(map(i -> i.size, table(x)))
        ),
        trades,
        :times)
    return agg_vol_price
end

function quote_compact(quote_path)
    quotes = remove_annomolies(keep_non_auction(loadtable(quote_path),5))
    most_recent = groupby(
        @NT(
            size = :size => x -> x[end],
            value = :value => x -> x[end]
        ),
        quotes,
        :times)
    return most_recent
end
j = 1
for i in 1:60
    if j == 7
        j = 1
    end
    asks = quote_compact(ask_files[i])
    bids = quote_compact(bid_files[i])
    sharename = ask_files[i][end-9:end-7]
    save(asks, string(dir_intday, "/asks/db", sharename,"00",j))
    save(bids, string(dir_intday, "/bids/db", sharename,"00",j))
    j = j + 1
end

for i in 1:10
    trades = trade_compact(trade_files[i])
    sharename = trade_files[i][end-6:end-4]
    save(trades, string(dir_intday, "/trades/db", sharename))
end
