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



a = trade_compact2(trade_files[1])
b = groupreduce(volume_weighted_price,a, :times)
b
keep_non_auction(b[1:end],10)


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

@inline function allequal(x)
    length(x) < 2 && return true
    e1 = x[1]
    i = 2
    @inbounds for i=2:length(x)
        x[i] == e1 || return false
    end
    return true
end

function quote_compact2(quote_path)
    quotes = loadtable(quote_path)
    unique_times = unique(select(quotes, :times))
    n = length(unique_times)
    values = Vector(n)
    size = Vector(n)
    for i in 1:n
        temp = filter(k -> k.times == unique_times[i])
        if allequal(select(temp, :value))
            size[i] = temp[end].size
            value[i] = temp[1].value
        elseif

    end

end

ask_files

asks = keep_non_auction(loadtable(ask_files[1]),10)
trades = keep_non_auction(loadtable(trade_files[1]), 10)
bids= keep_non_auction(loadtable(bid_files[1]),10)
unique_times = unique(select(quotes, :times))

select(a[1:20], :value)
a[20].size


ask_files[37]
function clean_intday_trades(path)
    trade_files = glob("*.csv",string(path, "/trades"))
    return trade_compact.(trade_files)
end

function classify_trades(share_code, path)
    trade_files = glob(string("*", share_code,".csv"),string(path, "/trades"))
    trades = trade_compact(trade_files)
end

function midprice(share_code, path)
    ask_files = glob(string("*", share_code,"*.csv"),string(path, "/asks"))
    bid_files = glob(string("*", share_code,"*.csv"),string(path, "/bids"))
    for i in 1:6
end
ask_files = glob(string("*", "NPN","*.csv"),string(dir_intday, "/asks"))

string("*","APN", ".csv")
@time all_trades = clean_intday_trades(dir_intday)



# 116.034881 seconds (467.46 M allocations: 20.059 GiB, 7.11% gc time)

@time trade_compact.(trade_files)





NPN_best_ask_prices = quote_compact(ask_files[37])
NPN_best_bid_prices = quote_compact(bid_files[37])
NPN_trades = trade_compact2(trade_files[7])

NPN_trades[1:5]



NPN_best_ask_prices = renamecol(NPN_best_ask_prices, :size, :ask_size)
NPN_best_ask_prices = renamecol(NPN_best_ask_prices, :value, :ask_value)

NPN_best_bid_prices = renamecol(NPN_best_bid_prices, :size, :bid_size)
NPN_best_bid_prices = renamecol(NPN_best_bid_prices, :value, :bid_value)


NPN_quote_data = join(NPN_best_ask_prices, NPN_best_bid_prices, how=:outer)

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
a = get_mid_price(NPN_quote_data)
b = pushcol(NPN_quote_data, :mid_price, a)


srand(1);

trades = table(rand(10^7), rand(10^7), rand(1:4, 10^7),
                      names=[:price,:volume,:date], pkey=:date);

function vol_weighted_avg3(a,b)
    ap, av, bp, bv = a.price, a.volume, b.price, b.volume
    @NT(price=(ap*av + bp*bv)/(av+bv), volume=av+bv)
end

groupreduce(vol_weighted_avg3, trades, :date)

c = loadtable(trade_files[1])

function vol_weighted_avg(a,b)
    return (a.value*a.size + b.value*b.size)/(a.size+b.size)
end
c
b = groupreduce(@NT(value=vol_weighted_avg), c[500:end], :times, select=(:size, :value))
a[500]
b[2][2].value
