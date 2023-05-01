
using ChannelBuffers: BClosure, BClosureList, closure, DEFAULT_IN, DEFAULT_OUT
using Base: AbstractCmd

dpath(x...) = joinpath(DDIR, x...)
tpath(x...) = joinpath(TDIR, x...)

bc = closure((cin, cout) -> nothing)
cmd = `ls`
file = "xxx"
@testset "constructor" begin
    pl = pipeline(bc, bc)
    @test pl == BClosureList(pl.list)
end
@testset "pipeline(...;stdin=$stdin, stdout=$stdout" for stdin in (nothing, "infile"),
                                                            stdout in (nothing, "outfile")
    if stdin === nothing && stdout === nothing
        @test pipeline(bc, stdin=stdin, stdout=stdout) === bc
        @test pipeline(cmd, stdin=stdin, stdout=stdout) === cmd
    else
        @test pipeline(bc, stdin=stdin, stdout=stdout) isa BClosureList
        @test pipeline(cmd, stdin=stdin, stdout=stdout) isa AbstractCmd
    end
end
@testset "pipeline($x, $y)" for x in (bc, cmd, file), y in (bc, cmd, file)
    if x === y === file
        @test length(pipeline(x, y).list) == 1
    elseif x === bc || y === bc
        @test pipeline(x, y) isa BClosureList
    else
        @test pipeline(x, y) isa AbstractCmd
    end
end
@testset "pipeline(1,2,3)" begin
    @test pipeline(bc, bc, bc) isa BClosureList
    @test pipeline(bc, bc, cmd) isa BClosureList
    @test pipeline(bc, cmd, bc) isa BClosureList
    @test pipeline(bc, cmd, cmd) isa BClosureList
    @test pipeline(cmd, bc, bc) isa BClosureList
    @test pipeline(cmd, bc, cmd) isa BClosureList
    @test pipeline(cmd, cmd, bc) isa BClosureList
    @test pipeline(cmd, cmd, cmd) isa Base.OrCmds
end

@testset "pipeline with ChannelIO" for p in (ChannelIO(), ChannelPipe())
    @test length(pipeline(cmd, p).list) == 2
    @test length(pipeline(p, cmd).list) == 2
    @test length(pipeline(pipeline(bc, cmd), p).list) == 3
    @test length(pipeline(p, pipeline(cmd, bc)).list) == 3
    @test length(pipeline(pipeline(bc, cmd), "file").list) == 2
    @test length(pipeline("file", pipeline(cmd, bc)).list) == 2
end
@testset "pipeline with list" begin
    @test length(pipeline(cmd, pipeline(bc, bc)).list) == 3
    @test length(pipeline(cmd, pipeline(cmd, bc)).list) == 2
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
