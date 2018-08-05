#PLotting

#Stocks NPN and CPI
#Times 10h00 and 11h00
#Times 16h00 and 17h00
using Gadfly
using JuliaDB
using IterableTables
using NamedColors
using StatPlots
using DataFrames

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"


trade_files = glob("cln_001*",string(dir_intday, "/clean_trades"))
ask_files = glob("db*",string(dir_intday, "/asks"))
bid_files = glob("db*",string(dir_intday, "/bids"))

load(trade_files[1])
ask_files[1]


function micro_price(share_code,path, start_time, end_time)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))

    asks = load(ask_files[1])
    bids = load(bid_files[1])
    ask_times = select(asks, :times)
    bid_times = select(bids, :times)
    uni_times = filter(j -> (j >= start_time && j <= end_time), unique(vcat(ask_times, bid_times)))
    mic_price = Vector{Float64}(length(uni_times))

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

function plot_data(share_code, path, start_time, end_time)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))
    trade_files = glob(string("cln_001_", share_code, "*"),string(path, "/clean_trades"))
    asks = load(ask_files[1])
    bids = load(bid_files[1])
    trades = load(trade_files[1])
    asks = filter(j -> (j.times <= end_time && j.times >= start_time), asks)
    bids = filter(j -> (j.times <= end_time && j.times >= start_time), bids)
    trades = filter(j -> (j.times <= end_time && j.times >= start_time), trades)

    return asks, bids, select(trades, (:times, :aggregate_volume, :volume_weighted_price))
end

NPN_micro_price1 = micro_price("NPN",dir_intday, DateTime(2018,01,29,10,00,00),DateTime(2018,01,29,11,00,00))

NPN_plot_asks, NPN_plot_bids, NPN_plot_trades = plot_data("NPN",dir_intday, DateTime(2018,01,29,10,00,00),DateTime(2018,01,29,11,00,00))
NPN_plot_asks = pushcol(NPN_plot_asks, :plot_size, select(NPN_plot_asks, :size)/mean(select(NPN_plot_asks, :size))*4)
select(NPN_plot_asks, :size)
NPN_micro_price2 = micro_price("NPN",dir_intday, DateTime(2018,01,29,16,00,00),DateTime(2018,01,29,17,00,00))

CPI_micro_price1 = micro_price("CPI",dir_intday, DateTime(2018,01,29,10,00,00),DateTime(2018,01,29,11,00,00))
CPI_micro_price2 = micro_price("CPI",dir_intday, DateTime(2018,01,29,16,00,00),DateTime(2018,01,29,17,00,00))

plotly()
plt1 = @df NPN_micro_price1 scatter(:times, :micro_price, color=:black, markersize =2,  bg=RGB(.2,.2,.2))
@df NPN_plot_asks scatter(:times, :value, color=:blue, markersize = :plot_size,  bg=RGB(.2,.2,.2), )
@df NPN_plot_bids scatter(:times, :value, color=:red, markersize = 4,  bg=RGB(.2,.2,.2))
gui(plt1)
gui
typeof(round(15.5,0))
data1 = [NPN_micro_price1, NPN_plot_asks]
scatter()


plot(x=rand(10), y=rand(10), dark_panel)
Gadfly.push_theme(:dark)
df = DataFrame(NPN_micro_price1)
select(NPN_micro_price1, :micro_price)

plot(NPN_micro_price1, x=:times, y=:micro_price,Geom.point)

Gadfly.push_theme(:dark)

plot1 = plot(
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("Micro Price for Naspers"),
    Scale.y_continuous(format=:plain),
    Theme(),
    layer(DataFrame(NPN_micro_price1),
        x = :times,
        y = :micro_price,
        Geom.point,
        style(default_color = colorant"black", highlight_width = 0pt,
            point_size=1.5pt)),
    layer(DataFrame(NPN_plot_asks),
        x = :times,
        y = :value,
        Geom.point,
        size = select(NPN_plot_asks, :size),
        style(default_color = colorant"blue", highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)),
    layer(DataFrame(NPN_plot_bids),
        x =:times,
        y =:value,
        Geom.point,
        size = select(NPN_plot_asks, :size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)),
    layer(DataFrame(NPN_plot_trades),
        x = :times,
        y = :volume_weighted_price,
        Geom.point,
        size = floor.(Int, select(NPN_plot_trades, :aggregate_volume)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)))




draw(PNG("myplot.png", 4inch, 3inch), plot1)


NPN_plot_trades
floor.(Int, select(NPN_plot_trades, :aggregate_volume))
Measures.Length.(select(NPN_plot_asks, :plot_size))
typeof(2pt)

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
