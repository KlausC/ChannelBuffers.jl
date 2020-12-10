using ChannelBuffers
using Test
using TranscodingStreams, CodecZlib

# reminder: @__MODULE__() === Main to check if executed by CI

if VERSION < v"1.5"
    peek = Base.peek
end

@testset "ChannelBuffers" begin
    @testset "ambiguities" begin
        @test detect_ambiguities(ChannelBuffers, Base) |> isempty
    end
    @testset "ChannelIO" begin include("channelio.jl") end 
    @testset "tasks    " begin include("tasks.jl") end
end

nothing