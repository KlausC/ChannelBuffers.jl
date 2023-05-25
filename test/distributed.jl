
@static if !isdefined(@__MODULE__, :P2)
    P2 = 2
    P3 = 3
end

using ChannelBuffers: channel_length, localchannel
using ChannelBuffers: RemoteChannelIODescriptor

function setup_pipe(closure)
    chdw = RemoteChannelIODescriptor(3)
    chdr = RemoteChannelIODescriptor(1, 1024)
    cw = ChannelIO(chdr, :W)
    cr = ChannelIO(chdw, :R)

    pl = pipeline(chdr, closure, chdw)
    fu = run(at(P3, pl), wait=false)
    return chdw, chdr, cw, cr, pl, fu
end

@testset "run at" begin
    chdw, chdr, cw, cr, pl, fu = setup_pipe(noop(10))
    data = "hallo\n" ^ 100
    write(cw, data)
    close(cw)
    @test read(cr, String) == data
end

#=
pl = ("LICENSE.md" → noop(9) → `sleep 5`)
res = remotecall(run, P2, pl)
fetch(res)
=#

@testset "remote channel_length" begin
    rc1 = RemoteChannel(()->Channel{Vector{UInt8}}(2), 1)
    put!(rc1, UInt8[1,2,3])
    put!(rc1, UInt8[4])
    @test channel_length(rc1) == (2, 4)
    ch = localchannel(rc1)
    @test channel_length(ch) == (2, 4)
    rc2 = RemoteChannel(()->Channel{Vector{UInt8}}(1), 2)
    @test channel_length(rc2) == (0, 0)
    @test_throws ArgumentError localchannel(rc2)
end

@testset "remote pipeline closures" begin
    cw = ChannelIO(:W)
    cr = ChannelIO(:R)
    pl = pipeline(cw, noop(11), at(P3, noop(12)), noop(13), cr)
    tl = run(pl, wait=false)
    data = "test data\n" ^10000
    @async begin write(cw, data); close(cw); end
    @test length(read(cr, String)) == length(data)
end
