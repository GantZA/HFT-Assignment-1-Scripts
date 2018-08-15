using StatsBase
using JuliaDB
using IterableTables
using Distributions
using Gadfly
using DataFrames
using Cairo
using Fontconfig
using NamedColors

dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

function get_tickers(path)
    return path[end-2:end]
end

function get_seconds(entry)
    return entry.value
end

function inter_arrival_time(share_code, path)
    trades = load(string(path, "/clean_trades/cln_001_", share_code))
    trade_dates = Date.(select(trades, :times))
    uni_trade_dates = unique(trade_dates)
    N = length(uni_trade_dates)
    delta_t = Vector{Int64}()
    start_ind = 1
    for i in 1:N
        end_ind = searchsortedlast(trade_dates,uni_trade_dates[i])
        temp_times = select(trades[start_ind:end_ind],:times)
        temp_diffs = get_seconds.(Dates.Second.(temp_times[2:end] .- temp_times[1:(end-1)]))
        start_ind = end_ind + 1
        append!(delta_t, temp_diffs)
    end
    return delta_t
end

tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))

all_delta_t = inter_arrival_time.(tickers, dir_intday)
@enter inter_arrival_time("NPN", dir_intday)
dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
for i in 1:10
    plt_delta_t = plot(x=all_delta_t[i], Scale.y_continuous,
        Guide.xlabel("Seconds"), Guide.ylabel("Frequency"), Guide.title(string(tickers[i]," Inter-Arrival Times")),
        Geom.histogram);
    draw(SVG(string(dir_plot, string("plot_delta_t_",tickers[i],".svg")), 16inch, 8inch), plt_delta_t)
end


########### The times or frequncy that is logged???
for i in 1:10
    plt_delta_t = plot(x=all_delta_t[i],
        Guide.xlabel("Seconds"), Guide.ylabel("Log Frequency"), Guide.title(string(tickers[i]," Log Inter-Arrival Times")),
        Geom.histogram, Scale.y_log);
    draw(SVG(string(dir_plot, string("plot_delta_logt_",tickers[i],".svg")), 16inch, 8inch), plt_delta_t)
end

for i in 1:10
    temp = DataFrame(ita = all_delta_t[i])
    mu = mean(temp[:ita])
    max_point = quantile(Exponential(mu), 1-(1/length(temp[:ita])))
    plot_temp = plot(Guide.xlabel("Theoretical Distribution"), Guide.ylabel("Empirical Distribution"), Guide.title(string(tickers[i]," Exponential QQ Plot")),
        layer(x = Exponential(mu),
        y = temp[:ita],
        Stat.qq,
        Geom.point,
        style(default_color = colorant"red",
        highlight_width = 0pt,
        point_size=1.5pt)),
        layer(
        x = [0, max_point], y = [0, max_point], Geom.line, style(default_color = colorant"black")
        )
    );
    draw(PNG(string(dir_plot, string("plot_qq_ita_", tickers[i], ".png")),16inch, 8inch), plot_temp)
    print(i)
end



all_delta_t_acf = autocor.(all_delta_t)

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
for i in 1:10
    plt_acf = plot(
        Guide.xlabel("Lag"), Guide.ylabel("Auto-Correlation"), Guide.title(string(tickers[i]," Inter-Arrival Times")),
        Guide.yticks(ticks=collect(0:0.1:1)),
        y=all_delta_t_acf[i], x=1:length(all_delta_t_acf[i]),
        Geom.point, yintercept = [ 1.96/sqrt(length(all_delta_t[i])) ], Geom.hline(style=:dot),
        style(
            default_color = colorant"green",
            highlight_width = 0pt,
            point_size=2pt));
    draw(SVG(string(dir_plot, string("plot_delta_t_acf_",tickers[i],".svg")), 16inch, 8inch), plt_acf)
end
