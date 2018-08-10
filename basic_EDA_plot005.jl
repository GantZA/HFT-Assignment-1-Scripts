using Gadfly
using JuliaDB
using IterableTables
using NamedColors
using DataFrames
using Cairo

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

function plot_data(share_code,path, start_time, end_time)
    ask_files = glob(string("db", share_code, "*"),string(path, "/asks"))
    bid_files = glob(string("db", share_code, "*"),string(path, "/bids"))
    trade_files = glob(string("cln_001_", share_code, "*"),string(path, "/clean_trades"))
    asks = load(ask_files[1])
    bids = load(bid_files[1])
    trades = load(trade_files[1])
    trades = filter(j -> (j.times >= start_time && j.times <= end_time), trades, select=(:times, :volume_weighted_price, :aggregate_volume))
    ask_times = select(asks, :times)
    bid_times = select(bids, :times)
    trade_times = select(trades, :times)
    N = length(trade_times)
    mic_price = Vector{Float64}(N)
    ask_plot_data = Matrix{Int64}(N,2)
    bid_plot_data = Matrix{Int64}(N,2)
    trade_plot_data = select(trades, (:volume_weighted_price, :aggregate_volume))
    for i in 1:N
        ask_range = searchsorted(ask_times, trade_times[i])
        bid_range = searchsorted(bid_times, trade_times[i])
        bid_index = start(bid_range) - 1
        ask_index = start(ask_range) - 1
        scale = (asks[ask_index].size)/(asks[ask_index].size + bids[bid_index].size)
        mic_price[i] = round( scale*asks[ask_index].value + (1-scale)*bids[bid_index].value ,2)
        ask_plot_data[i,:] = [asks[ask_index].value,asks[ask_index].size]
        bid_plot_data[i,:] = [bids[bid_index].value,bids[bid_index].size]
    end
    return table(@NT(:times=trade_times,:micro_price = mic_price,
        :ask_value =ask_plot_data[:,1], :ask_size = ask_plot_data[:,2],
        :bid_value = bid_plot_data[:,1], :bid_size = bid_plot_data[:,2],
        :trade_value =select(trade_plot_data, :volume_weighted_price),
        :trade_size = select(trade_plot_data, :aggregate_volume)))
end

NPN_plot_data001 = plot_data("NPN",dir_intday, DateTime(2018,01,29,10,00,00),
    DateTime(2018,01,29,11,00,00))

plot_NPN_001 = plot(NPN_plot_data001,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("10AM-11AM Naspers Trading"),
    Scale.y_continuous(format=:plain),
    Guide.manual_color_key("Legend", ["Micro Price", "Best Ask","Best Bid", "Trade"], ["black", "blue", "red", "yellow"]),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        Stat.yticks(coverage_weight = 0.9),
        style(default_color = colorant"black", highlight_width = 0pt,
            point_size=1.5pt)),

    layer(
        x = :times,
        y = :trade_value,
        Geom.point,
        size = floor.(Int, select(NPN_plot_data001, :trade_size)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt
        )
    ),
    layer(
        x = :times,
        y = :ask_value,
        Geom.point,
        size = select(NPN_plot_data001, :ask_size),
        style(default_color = colorant"blue", highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)
    ),
    layer(
        x =:times,
        y =:bid_value,
        Geom.point,
        size = select(NPN_plot_data001, :bid_size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt))
    );

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot_NPN_001.svg"), 16inch, 9inch), plot_NPN_001)

NPN_plot_data002 = plot_data("NPN",dir_intday, DateTime(2018,01,29,16,00,00),
    DateTime(2018,01,29,17,00,00))

plot_NPN_002 = plot(NPN_plot_data002,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("4PM-5PM Naspers Trading"),
    Scale.y_continuous(format=:plain),
    Guide.manual_color_key("Legend", ["Micro Price", "Best Ask","Best Bid", "Trade"], ["black", "blue", "red", "yellow"]),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        Stat.yticks(coverage_weight = 0.9),
        style(default_color = colorant"black", highlight_width = 0pt,
            point_size=1.5pt)),

    layer(
        x = :times,
        y = :trade_value,
        Geom.point,
        size = floor.(Int, select(NPN_plot_data002, :trade_size)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt
        )
    ),
    layer(
        x = :times,
        y = :ask_value,
        Geom.point,
        size = select(NPN_plot_data002, :ask_size),
        style(default_color = colorant"blue", highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)
    ),
    layer(
        x =:times,
        y =:bid_value,
        Geom.point,
        size = select(NPN_plot_data002, :bid_size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt))
    );

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot_NPN_002.svg"), 16inch, 9inch), plot_NPN_002)

CPI_plot_data001 = plot_data("CPI",dir_intday, DateTime(2018,01,29,10,00,00),
    DateTime(2018,01,29,11,00,00))

plot_CPI_001 = plot(CPI_plot_data001,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("10AM-11AM Capitec Trading"),
    Scale.y_continuous(format=:plain),
    Guide.manual_color_key("Legend", ["Micro Price", "Best Ask","Best Bid", "Trade"], ["black", "blue", "red", "yellow"]),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        Stat.yticks(coverage_weight = 0.9),
        style(default_color = colorant"black", highlight_width = 0pt,
            point_size=1.5pt)),

    layer(
        x = :times,
        y = :trade_value,
        Geom.point,
        size = floor.(Int, select(CPI_plot_data001, :trade_size)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt
        )
    ),
    layer(
        x = :times,
        y = :ask_value,
        Geom.point,
        size = select(CPI_plot_data001, :ask_size),
        style(default_color = colorant"blue", highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)
    ),
    layer(
        x =:times,
        y =:bid_value,
        Geom.point,
        size = select(CPI_plot_data001, :bid_size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt))
    );

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot_CPI_001.svg"), 16inch, 9inch), plot_CPI_001)

CPI_plot_data002 = plot_data("CPI",dir_intday, DateTime(2018,01,29,16,00,00),
    DateTime(2018,01,29,17,00,00))

plot_CPI_002 = plot(CPI_plot_data002,
    Guide.xlabel("Time"), Guide.ylabel("Price"), Guide.title("4PM-5PM Capitec Trading"),
    Scale.y_continuous(format=:plain),
    Guide.manual_color_key("Legend", ["Micro Price", "Best Ask","Best Bid", "Trade"], ["black", "blue", "red", "yellow"]),
    layer(
        x = :times,
        y = :micro_price,
        Geom.point,
        Stat.yticks(coverage_weight = 0.9),
        style(default_color = colorant"black", highlight_width = 0pt,
            point_size=1.5pt)),

    layer(
        x = :times,
        y = :trade_value,
        Geom.point,
        size = floor.(Int, select(CPI_plot_data002, :trade_size)),
        style(default_color = colorant"yellow",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt
        )
    ),
    layer(
        x = :times,
        y = :ask_value,
        Geom.point,
        size = select(CPI_plot_data002, :ask_size),
        style(default_color = colorant"blue", highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt)
    ),
    layer(
        x =:times,
        y =:bid_value,
        Geom.point,
        size = select(CPI_plot_data002, :bid_size),
        style(default_color = colorant"red",highlight_width = 0pt,
            point_size_min = 1.5pt, point_size_max = 8pt))
    );

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot_CPI_002.svg"), 16inch, 9inch), plot_CPI_002)
