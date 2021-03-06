#ACF

using StatsBase
using JuliaDB
using NamedColors
using Gadfly

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

function acf_order_flow(share_code, path)
    trades = load(string(dir_intday, "/clean_trades/cln_001_", share_code))
    trade_signs = Vector{Int32}(select(trades, :trade_sign))
    acf = autocor(trade_signs)
    return acf, length(trades)
end

function get_tickers(path)
    return path[end-2:end]
end



tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))
all_acf = acf_order_flow.(tickers, dir_intday)
all_acf[1]
dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"

for i in 1:10
    ub = 1.96/sqrt(all_acf[i][2])
    plt_acf = plot(
        Guide.xlabel("Lag"), Guide.ylabel("Auto-Correlation"), Guide.title(string(tickers[i]," Trading Signs")),
        y=all_acf[i][1], x=0:(length(all_acf[i][1])-1), yintercept = [ub], Geom.hline(style=:dot),
        Geom.point,
        style(
            default_color = colorant"green",
            highlight_width = 0pt,
            point_size=2pt));
    draw(SVG(string(dir_plot, string("plot_acf_001_",tickers[i],".svg")), 16inch, 8inch), plt_acf)
end



NPN_trades = load(string(dir_intday, "/clean_trades/cln_001_NPN"))
NPN_trade_signs = Vector{Int32}(select(NPN_trades, :trade_sign))
typeof(NPN_trade_signs)
acf = autocor(NPN_trade_signs)

plt_acf = plot(
    Guide.xlabel("Lag"), Guide.ylabel("Auto-Correlation"), Guide.title("Naspers Trading Signs"),
    y=acf,
    x=1:58,
    Geom.point,
    style(
        default_color = colorant"green",
        highlight_width = 0pt,
        point_size=2pt))

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
draw(SVG(string(dir_plot, "plot_acf_001.svg"), 16inch, 8inch), plt_acf)
