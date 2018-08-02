using JuliaDB
using CSV


dir_ds = "/Documents/UCT 2018/HFT/Assignment 1/data/daily_sampled"
dir_intday = "/Documents/UCT 2018/HFT/Assignment 1/data/intraday"

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

NPN_trades = trade_compact(trade_files[7])

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

function get_mid_price(share_code,path)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))
    trade_files = glob(string("db", share_code,"*"),string(path, "/trades"))

    file_index = 1
    trades = load(trade_files[1])
    asks = load(ask_files[file_index])
    bids = load(bid_files[file_index])
    ask_dates = select(asks, :times)
    n = length(trades)
    #before_mid_price, after_mid_price, change_mid_price, bid_price, bid_volume,
        #ask_price, ask_volume, trade_sign = Vector(n), Vector(n),Vector(n),
        #Vector(n),Vector(n),Vector(n),Vector(n),Vector(n)
    metrics = Matrix(n,8)
    for i in 1:n
        trade_time = trades[i].times
        if length(filter(j -> j.times < trade_time, asks)) == 0
            file_index = file_index + 1
            asks = quote_compact(ask_files[file_index])
            bids = quote_compact(bid_files[file_index])
            before_ask = filter(j -> j.times < trade_time, asks)[end]
        end
        before_ask = filter(j -> j.times < trade_time, asks)[end]
        after_ask = filter(j -> j.times >= trade_time,asks)[end]
        before_bid = filter(j -> j.times < trade_time,bids)[end]
        after_bid = filter(j -> j.times >= trade_time,bids)[end]

        metrics[i,1] = (before_ask.value - before_bid.value)/2
        metrics[i,2] = (after_ask.value - after_bid.value)/2
        metrics[i,3] = metrics[i,2] - metrics[i,1]
        metrics[i,4] = before_bid.value
        metrics[i,5] = before_bid.size
        metrics[i,6] = before_ask.value
        metrics[i,7] = before_ask.size
        if trades[i].volume_weighted_price > metrics[i,1]
            metrics[i,8] = 1
        else
            metrics[i,8] = -1
        end
    end
    ind_names = [:before_mid_price, :after_mid_price, :change_mid_price, :bid_price,:bid_volume, :ask_price, :ask_volume, :trade_sign]
    for i in 1:8
        trades = pushcol(trades,names[i], metrics[:,i])
    end
    return trades
end
NPN_trades = get_mid_price("NPN", dir_intday)
NPN_trades
print(Dates.Time(Dates.now()))
#23:37

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

before_mid_price, after_mid_price, change_mid_price, bid_price, bid_volume,
    ask_price, ask_volume, trade_sign = Vector(n), Vector(n),Vector(n),
    Vector(n),Vector(n),Vector(n),Vector(n),Vector(n)
