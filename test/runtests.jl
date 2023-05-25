using ChannelBuffers
using Test
using TranscodingStreams, CodecZlib

# reminder: @__MODULE__() === Main to check if executed by CI

if VERSION < v"1.5"
    peek = Base.peek
end

const DDIR = abspath(dirname(@__FILE__),"..", "data", "test")
const TDIR = mktempdir(cleanup=false)

dpath(x...) = joinpath(DDIR, x...)
tpath(x...) = joinpath(TDIR, x...)

println("testing tmpdir is $TDIR test data from $DDIR")
println("pwd() = $(pwd())")
println("JULIA_NUM_THREADS=$(Threads.nthreads())")

@testset "ChannelBuffers" begin
    @testset "ambiguities" begin
        @test detect_ambiguities(ChannelBuffers) |> isempty
        VERSION >= v"1.8" && @test detect_unbound_args(ChannelBuffers) |> isempty
    end
#  @testset "ChannelIO  " begin include("channelio.jl") end
# println("finished ChannelIO")
#  @testset "pipelines  " begin include("pipelines.jl") end
#  println("finished pipelines")
    @testset "tasks      " begin include("tasks.jl") end
    println("finished tasks")
    @testset "distributed" begin include("runtestsdist.jl") end
    println("finished distributed")
end

nothing
