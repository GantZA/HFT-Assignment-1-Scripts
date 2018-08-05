using Gadfly
using JuliaDB
using IterableTables
using NamedColors
using DataFrames
using Cairo

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

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

plot1 = plot(
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("10AM-11AM Naspers Trading"),
    Scale.y_continuous(format=:plain),
    Guide.manual_color_key("Legend", ["Micro Price", "Best Ask","Best Bid", "Trade"], ["black", "blue", "red", "yellow"]),
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
        size = select(NPN_plot_bids, :size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)),
    layer(DataFrame(NPN_plot_trades),
        x = :times,
        y = :volume_weighted_price,
        Geom.point,
        size = floor.(Int, select(NPN_plot_trades, :aggregate_volume)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)
    )
);

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot1.svg"), 16inch, 9inch), plot1)
