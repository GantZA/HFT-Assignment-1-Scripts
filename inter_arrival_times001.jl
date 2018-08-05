using StatsBase
using JuliaDB
using Distributions
using Gadfly
dir_ds = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/daily_sampled"
dir_intday = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/data/intraday"

function get_tickers(path)
    return path[end-2:end]
end

function get_seconds(entry)
    return entry.value
end

function inter_arrival_time(share_code, path)
    trades = load(string(dir_intday, "/clean_trades/cln_001_", share_code))
    trade_dates = Date.(select(trades, :times))
    N = length(unique(trade_dates))
    delta_t = Vector{Int64}()
    start_ind = 1
    for i in 1:N
        end_ind = searchsortedlast(trade_dates,trade_dates[i])
        temp_times = select(trades[start_ind:end_ind],:times)
        temp_diffs = get_seconds.(Dates.Second.(temp_times[2:end] .- temp_times[1:(end-1)]))
        append!(delta_t, temp_diffs)
    end
    return delta_t
end

tickers = get_tickers.(glob(string("db","*"),string(dir_intday, "/trades")))

all_delta_t = inter_arrival_time.(tickers, dir_intday)

dir_plot = "C:/Users/USER/Documents/Michael Gant/HFT/Assignment 1/plots/"
for i in 1:10
    plt_delta_t = plot(x=all_delta_t[i],
        Guide.xlabel("Seconds"), Guide.ylabel("Frequency"), Guide.title(string(tickers[i]," Inter-Arrival Times")),
        Geom.histogram);
    draw(SVG(string(dir_plot, string("plot_delta_t_",tickers[i],".svg")), 16inch, 8inch), plt_delta_t)
end



for i in 1:10
    plt_delta_t = plot(x=all_delta_t[i],
        Guide.xlabel("Log Seconds"), Guide.ylabel("Frequency"), Guide.title(string(tickers[i]," Log Inter-Arrival Times")),
        Geom.histogram, Scale.y_log);
    draw(SVG(string(dir_plot, string("plot_delta_logt_",tickers[i],".svg")), 16inch, 8inch), plt_delta_t)
end


using StatPlots
using Gadfly
rand(Exponential() ,100)
a = collect(0:1/(length(all_delta_t[1])):1)
quantile.(Exponential(), a)

for i in 1:10
    qq_plt = qqplot(Exponential, all_delta_t[i],
    Guide.xlabel("Theoretical Exponential Quantiles"),
    Guide.ylabel("Empirical Quantiles"),
    Guide.title(string(tickers[i]," QQ Plot")));
    draw(SVG(string(dir_plot, string("plot_qq_",tickers[i],".svg")), 16inch, 8inch), qq_plt)
end
test = qqplot(Exponential, all_delta_t[1][1:100]);
draw(SVG(string(dir_plot, string("plot_test_",tickers[1],".svg")), 16inch, 8inch), test)


all_delta_t[1][1:50]






function QQ_plot_IAT(iat)
    sorted_data = sort(iat)
    N = length(iat)
    z_k = collect(0:1/N:1)
    mu = mean(iat)
    y_k = quantile.(Exponential, z_k)
    
end

sort(all_delta_t[1])
length(all_delta_t[1])
0:length(all_delta_t[1]):1
collect(1:length(all_delta_t[1]))/(length(all_delta_t[1]))
collect(0:1/(length(all_delta_t[1])):1)
@time NPN_delta_t = inter_arrival_time("NPN", dir_intday)
NPN_delta_t
plt_delta_t = plot(x=NPN_delta_t,
    Guide.xlabel("Seconds"), Guide.ylabel("Frequency"), Guide.title("Naspers Inter-Arrival Times"),
    Geom.histogram)

counts(NPN_delta_t, 1:124)
maximum(NPN_delta_t)
NPN_delta_t[NPN_delta_t.==0]
sum(NPN_delta_t.>0)
NPN_delta_t
