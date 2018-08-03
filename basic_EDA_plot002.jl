#PLotting

#Stocks NPN and CPI
#Times 10h00 and 11h00
#Times 16h00 and 17h00
using Gadfly
using JuliaDB
using IterableTables
using NamedColors

dir_ds = "/Documents/UCT 2018/HFT/Assignment 1/data/daily_sampled"
dir_intday = "/Documents/UCT 2018/HFT/Assignment 1/data/intraday"

trade_files = glob("*.csv",string(dir_intday, "/trades"))
ask_files = glob("*.csv",string(dir_intday, "/asks"))
bid_files = glob("*.csv",string(dir_intday, "/bids"))

function micro_price(share_code,path, start_time, end_time)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))

    asks = load(ask_files[1])
    bids = load(bid_files[1])
    ask_times = select(asks, :times)
    bid_times = select(bids, :times)
    uni_times = filter(j -> (j >= start_time && j <= end_time), unique(vcat(ask_times, bid_times)))
    mic_price = Vector(length(uni_times))

    for i in 1:length(uni_times)
        ask_range = searchsorted(ask_times, uni_times[i])
        bid_range = searchsorted(bid_times, uni_times[i])
        if done(bid_range,start(bid_range))
            bid_index = start(bid_range) - 1
        else
            bid_index = start(bid_range)
        end

        if done(ask_range,start(ask_range))
            ask_index = start(ask_range) - 1
        else
            ask_index = start(bid_range)
        end
        mic_price[i] = round((asks[ask_index].size*asks[ask_index].value + bids[bid_index].size*bids[bid_index].value)/(asks[ask_index].size + bids[bid_index].size),2)
    end
    return table(@NT(:times=uni_times,:micro_price = mic_price))
end
NPN_micro_price1 = micro_price("NPN",dir_intday, DateTime(2018,01,29,10,00,00),DateTime(2018,01,29,11,00,00))
NPN_micro_price2 = micro_price("NPN",dir_intday, DateTime(2018,01,29,16,00,00),DateTime(2018,01,29,17,00,00))

CPI_micro_price1 = micro_price("CPI",dir_intday, DateTime(2018,01,29,10,00,00),DateTime(2018,01,29,11,00,00))
CPI_micro_price2 = micro_price("CPI",dir_intday, DateTime(2018,01,29,16,00,00),DateTime(2018,01,29,17,00,00))

Gadfly.push_theme(:dark)

plot1 = plot(
    NPN_micro_price1,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("Micro Price for Naspers"),
    Scale.y_continuous(format=:plain),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        style(default_color = colorant"black", highlight_width = 0pt)))
plot2 =  plot(
    NPN_micro_price2,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("Micro Price for Naspers"),
    Scale.y_continuous(format=:plain),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        style(default_color = colorant"black", highlight_width = 0pt)))






trade_files = glob(string("db", "NPN","*"),string(dir_intday, "/trades"))
ask_files = glob(string("db", "NPN", "*"),string(dir_intday, "/asks"))
bid_files = glob(string("db", "NPN", "*"),string(dir_intday, "/bids"))
NPN_trades = load(trade_files[1])
filter(i -> i.aggregate_volume == 0, NPN_trades)
NPN_asks = load(ask_files[1])
NPN_bids = load(bid_files[1])
NPN_asks[1].times
DateTime("2018-01-29:09:05:02")

DateTime(2018,01,29,09,05,02)

start_time = DateTime(2018,01,29,10,00,00)
end_time = DateTime(2018,01,29,11,00,00)

NPN_asks = load(ask_files[1])
NPN_bids = load(bid_files[1])
ask_times = select(NPN_asks, :times)
bid_times = select(NPN_bids, :times)
uni_times = filter(j -> (j >= start_time && j <= end_time), unique(vcat(ask_times, bid_times)))
mic_price = Vector(length(uni_times))

for i in 1:length(uni_times)
    ask_range = searchsorted(ask_times, uni_times[i])
    bid_range = searchsorted(bid_times, uni_times[i])
    if done(bid_range,start(bid_range))
        bid_index = start(bid_range) - 1
    else
        bid_index = start(bid_range)
    end

    if done(ask_range,start(ask_range))
        ask_index = start(ask_range) - 1
    else
        ask_index = start(bid_range)
    end
    mic_price[i] = round((NPN_asks[ask_index].size*NPN_asks[ask_index].value + NPN_bids[bid_index].size*NPN_bids[bid_index].value)/(NPN_asks[ask_index].size + NPN_bids[bid_index].size),2)
end
table(uni_times,mic_price)
ask_index = searchsorted(ask_times, uni_times[1])
bid_index = searchsorted(bid_times, uni_times[1])
done(bid_index,start(bid_index))
done(ask_index,start(ask_index))
