using Distributed
addprocs(1)
@everywhere begin
    using Pkg
    Pkg.activate(".")
end
println(fetch(@spawnat(2, begin io = IOBuffer(); Pkg.status(;io); String(take!(io)); end)))
@everywhere begin
    using ChannelBuffers
    using ChannelBuffers: BClosure, BClosureList
end
pl = ("LICENSE.md" → noop() → "L2.md")
res = remotecall(run, 2, pl)
fetch(res)
