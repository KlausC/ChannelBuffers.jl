using ChannelBuffers
using Test
using TranscodingStreams, CodecZlib

# reminder: @__MODULE__() === Main to check if executed by CI

@testset "tasks" begin include("tasks.jl") end

@testset "ChannelIO" begin include("ChannelIO.jl") end
