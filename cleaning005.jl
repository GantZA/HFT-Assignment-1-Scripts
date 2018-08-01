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

function trade_compact(trade_path)
    trades =  keep_non_auction(loadtable(trade_path),10)
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
    quotes = keep_non_auction(loadtable(quote_path),5)
    most_recent = groupby(
        @NT(
            size = :size => x -> x[end],
            value = :value => x -> x[end]
        ),
        quotes,
        :times)
    return most_recent
end

#Consider NPN

NPN_trades = keep_non_auction(loadtable(trade_files[7]), 10)
NPN_asks = keep_non_auction(loadtable(ask_files[37]), 5)
NPN_bids = keep_non_auction(loadtable(bid_files[37]), 10)

function get_mid_price(quotes, trades)
    for each in trades
        ind = length
        mid_before = ()/2
    end


end

function get_mid_price(quotes)
    best_ask = quotes[1].ask_value.value
    best_bid = quotes[1].bid_value.value
    mid_price = Vector(length(quotes))
    mid_price[1] = (best_ask - best_bid)/2
    for i in 2:length(quotes)
        if quotes[i].ask_value.value > 0
            best_ask = quotes[i].ask_value.value
        end
        if quotes[i].bid_value.value > 0
            best_bid = quotes[i].bid_value.value
        end
        mid_price[i] = (best_ask - best_bid)/2
    end
    return mid_price
end

                                                            NPN_trades[1:10]





NPN_asks[1:10]












                                                            NPN_bids[1:22]
