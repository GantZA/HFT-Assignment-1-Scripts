using JuliaDB
using CSV


dir_ds = "/Documents/UCT 2018/HFT/Assignment 1/data/daily_sampled"
dir_intday = "/Documents/UCT 2018/HFT/Assignment 1/data/intraday"


function get_tickers(path)
    return path[end-2:end]
end
tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))

function get_mid_price(share_code,path)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))
    trade_files = glob(string("db", share_code,"*"),string(path, "/trades"))

    file_index = 1
    trades = load(trade_files[1])
    asks = load(ask_files[file_index])
    bids = load(bid_files[file_index])
    ask_dates = select(asks, :times)
    bid_dates = select(bids, :times)
    trade_dates = select(trades, :times)
    n = length(trades)
    #before_mid_price, after_mid_price, change_mid_price, bid_price, bid_volume,
        #ask_price, ask_volume, trade_sign = Vector(n), Vector(n),Vector(n),
        #Vector(n),Vector(n),Vector(n),Vector(n),Vector(n)
    metrics = Matrix(n,8)
    end_i = maximum([searchsortedfirst(trade_dates, ask_dates[end]), searchsortedfirst(trade_dates, bid_dates[end])])
    for i in 1:n
        trade_time = trades[i].times

        if i > end_i
            file_index = file_index + 1
            asks = load(ask_files[file_index])
            bids = load(bid_files[file_index])
            ask_dates = select(asks, :times)
            bid_dates = select(bids, :times)

            ask_index = searchsorted(ask_dates, trade_time)
            bid_index = searchsorted(bid_dates, trade_time)
            ask_index_first = start(ask_index)
            bid_index_first = start(bid_index)

            end_i = maximum([searchsortedfirst(trade_dates, ask_dates[end]), searchsortedfirst(trade_dates, bid_dates[end])])
        else
            ask_index = searchsorted(ask_dates, trade_time)
            bid_index = searchsorted(bid_dates, trade_time)
            ask_index_first = start(ask_index)
            bid_index_first = start(bid_index)
        end


        before_ask = asks[ask_index_first-1]
        before_bid = bids[bid_index_first-1]

        if done(ask_index, ask_index_first)
            after_ask = asks[ask_index_first-1]
        else
            after_ask = asks[ask_index_first]
        end

        if done(bid_index, bid_index_first)
            after_bid = bids[bid_index_first-1]
        else
            after_bid = bids[bid_index_first]
        end

        metrics[i,1] = round((before_ask.value - before_bid.value)/2,2)
        metrics[i,2] = round((after_ask.value - after_bid.value)/2,2)
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
        trades = pushcol(trades,ind_names[i], metrics[:,i])
    end
    return trades
end


tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))
@time all_trades = get_mid_price.(tickers, dir_intday)
# 25.265582 seconds (76.15 M allocations: 1.776 GiB, 84.71% gc time)


for i in 1:10
    save(all_trades[i], string(dir_intday, "/clean_trades/cln", tickers[i]))
end
