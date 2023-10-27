
using ChannelBuffers: BClosure, BClosureList, closure, DEFAULT_IN, DEFAULT_OUT
using Base: AbstractCmd

bcl = closure((cin, cout) -> nothing)
cmd = `ls`
file = "xxx"
@testset "constructor" begin
    pl = pipeline(bcl, bcl)
    @test pl == BClosureList(pl.list)
end
@testset "pipeline(...;stdin=$stdin, stdout=$stdout" for stdin in (nothing, "infile"),
                                                            stdout in (nothing, "outfile")
    if stdin === nothing && stdout === nothing
        @test pipeline(bcl, stdin=stdin, stdout=stdout) === bcl
        @test pipeline(cmd, stdin=stdin, stdout=stdout) === cmd
    else
        @test pipeline(bcl, stdin=stdin, stdout=stdout) isa BClosureList
        @test pipeline(cmd, stdin=stdin, stdout=stdout) isa AbstractCmd
    end
end
@testset "pipeline($x, $y)" for x in (bcl, cmd, file), y in (bcl, cmd, file)
    if x === y === file
        @test_throws MethodError pipeline(x, y)
    elseif x === bcl || y === bcl
        @test pipeline(x, y) isa BClosureList
    else
        @test pipeline(x, y) isa AbstractCmd
    end
end
@testset "pipeline(1,2,3)" begin
    @test pipeline(bcl, bcl, bcl) isa BClosureList
    @test pipeline(bcl, bcl, cmd) isa BClosureList
    @test pipeline(bcl, cmd, bcl) isa BClosureList
    @test pipeline(bcl, cmd, cmd) isa BClosureList
    @test pipeline(cmd, bcl, bcl) isa BClosureList
    @test pipeline(cmd, bcl, cmd) isa BClosureList
    @test pipeline(cmd, cmd, bcl) isa BClosureList
    @test pipeline(cmd, cmd, cmd) isa Base.OrCmds
end

@testset "pipeline with ChannelIO" for p in (ChannelIO(), ChannelPipe())
    @test length(pipeline(cmd, p).list) == 2
    @test length(pipeline(p, cmd).list) == 2
    @test length(pipeline(pipeline(bcl, cmd), p).list) == 3
    @test length(pipeline(p, pipeline(cmd, bcl)).list) == 3
    @test length(pipeline(pipeline(bcl, cmd), "file").list) == 2
    @test length(pipeline("file", pipeline(cmd, bcl)).list) == 2
end
@testset "pipeline with list" begin
    @test length(pipeline(cmd, pipeline(bcl, bcl)).list) == 3
    @test length(pipeline(cmd, pipeline(cmd, bcl)).list) == 2
end

@testset "show tasklist" begin
    pl = (IOBuffer("hallo") → (gzip() → gunzip()) → (gzip() → gunzip()) → devnull)
    tl = run(pl)
    @test sprint(show, MIME"text/plain"(), tl) |> length > 10
end

@testset "file to file" begin
    pl = "xxx" → "yyy"
    @test length(pl.list) == 1
end
