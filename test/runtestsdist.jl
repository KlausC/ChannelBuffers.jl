using Distributed
addprocs(2)
#addprocs([("127.0.0.1", 1)])
@everywhere begin
    using Pkg
    Pkg.activate("..")
end
println(fetch(@spawnat(2, begin io = IOBuffer(); Pkg.status(;io); String(take!(io)); end)))
@everywhere begin
    using ChannelBuffers
end

include("distributed.jl")

rmprocs(2, 3)
