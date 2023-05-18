using Distributed
using ChannelBuffers
#cd(pkgdir(ChannelBuffers, "test"))
addprocs(2)
PROCS = procs()
P2 = PROCS[end-1]
P3 = PROCS[end]
#println("procs = ", PROCS)
#addprocs([("127.0.0.1", 1)])
#=
@everywhere begin
    using Pkg
    Pkg.activate("..")
end
println(fetch(@spawnat(P2, begin io = IOBuffer(); Pkg.status(;io); String(take!(io)); end)))
=#
@everywhere P2, P3 begin
    using ChannelBuffers
end

include("distributed.jl")

#rmprocs(P2, P3)
