using ChannelBuffers
using Test
using TranscodingStreams, CodecZlib

# reminder: @__MODULE__() === Main to check if executed by CI

@testset "ChannelBuffers" begin
    @testset "ChannelIO" begin include("channelIO.jl") end 
    @testset "tasks    " begin include("tasks.jl") end
end