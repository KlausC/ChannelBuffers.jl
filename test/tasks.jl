
using ChannelBuffers: BClosure, BClosureList, closure, DEFAULT_IN, DEFAULT_OUT
using ChannelBuffers: BTask, task_function, task_cin, task_cout, task_args, NOOP
using Base: AbstractCmd

dpath(x...) = joinpath(DDIR, x...)
tpath(x...) = joinpath(TDIR, x...)

@testset "pipelines" begin 
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
        @test pipeline(bcl, cmd, bcl) isa BClosureList
        @test pipeline(cmd, bcl, cmd) isa BClosureList
        @test pipeline(bcl, bcl, cmd) isa BClosureList
        @test pipeline(bcl, cmd, cmd) isa BClosureList
        @test pipeline(bcl, bcl, bcl) isa BClosureList
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
end

@testset "run individual task" begin
    
    file = tpath("tfile")
    text = "This is a small test data file"
    open(file, "w") do io
        write(io, text)
    end
    io = IOContext(IOBuffer())
    run(source(file), stdout = io) |> wait
    @test String(take!(io.io)) == text
end

@testset "serialize and deserialize" begin
    obj = ["hans", 92]
    tl = run(serializer(obj) | deserializer())
    @test fetch(tl) == obj
    tl = run(serializer(obj) | gzip() | deserializer())
    @test_throws TaskFailedException fetch(tl)
end

@testset "tar -c" begin
    file = tpath("xxx.tgz")
    tar = tarc(dpath("xxx")) | gzip() | destination(file)
    tl = run(tar)
    @test length(tl) == 3
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl))
    @test all(istaskdone.(tl))
    @test !any(istaskfailed.(tl))
    @test sprint(show, tl) !== nothing
end

@testset "tar -c using pipeline" begin
    file = tpath("xxx0.tgz")
    tar = pipeline(tarc(dpath("xxx")), gzip(), open(file, "w"))
    tl = run(tar)
    close(tar.cout)
    @test length(tl) == 2
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl))
    @test !any(istaskfailed.(tl))
end

raw"""
just in case we allow Channel
@testset "redirect to Channel" begin
    fin = tpath("xxx0.tgz")
    fout = Channel(100)
    pl = pipeline(source(fin), fout)
    tl = run(pl)
    while isready(fout)
        take!(fout)
    end
    @test wait(tl) === nothing
end
"""

@testset "tar -x $dir" for (decoder, dir) in [(gunzip(), "xxx"), (transcoder(GzipDecompressor()), "xxx2")]
    file = tpath("xxx1.tgz")
    tar = tarc(dpath("xxx")) → gzip() → open(file, "w")
    tar |> run |> wait
    close(tar.cout)
    tar = source(file) | decoder | tarx(tpath(dir))
    tl = run(tar)
    @test length(tl) == 3
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl))
    @test !any(istaskfailed.(tl))

    dc = `diff -r "$DDIR/xxx" "$TDIR/$dir"` # check if contents of both dirs are equal
    @test run(dc) !== nothing
end

@testset "copy file" begin
    cpy = source(tpath("xxx.tgz")) | destination(tpath("xxx2.tgz")) → stdout
    tl = run(cpy)
    @test length(tl) == 2
    @test tl[2] isa BTask
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl))
    @test !any(istaskfailed.(tl))
    fc = `diff -r "$TDIR/xxx.tgz" "$TDIR/xxx2.tgz"` # check if contents of both dirs are equal
    @test run(fc) !== nothing
    bt = tl[2]
    @test task_cin(bt) isa IO
    @test task_cout(bt) isa IO
    @test task_function(bt) isa Function
    @test task_args(bt) isa Tuple{String}
end

@testset "downloads and output redirection" begin
    open(tpath("xxx3.tgz"), "w") do io
        wait(run(curl("file://" * TDIR * "/xxx.tgz") → io))
    end
    fc = `diff "$TDIR/xxx.tgz" "$TDIR/xxx3.tgz"`
    @test run(fc) !== nothing
end

@testset "destination and input redirection" begin
    open(tpath("xxx.tgz"), "r") do io
        (io → destination(tpath("xxx4.tgz"))) |> run |> wait
    end
    fc = `diff "$TDIR/xxx.tgz" "$TDIR/xxx4.tgz"`
    @test run(fc) !== nothing
end

@testset "io redirection with pipeline" begin
    fin = tpath("xxx4.tgz")
    fout = tpath("xxx5.tgz")
    (fin → gunzip() | gzip() → fout) |> run |> wait
    fc = `diff "$fin" "$fout"`
    @test run(fc) !== nothing
end

@testset "show tasklist" begin
    pl = (IOBuffer("hallo") → (gzip() → gunzip()) → (gzip() → gunzip()) → devnull)
    tl = run(pl)
    @test sprint(show, MIME"text/plain"(), tl) |> length > 10
end

@testset "mixed pipline run" begin
    fout = tpath("xxx.txt")
    pl = pipeline(`ls ../src`, NOOP, `cat -`, fout)
    tl = run(pl)
    @test wait(tl) === nothing
    @test run(pipeline(`ls ../src`, `cmp - $fout`)) !== nothing
end

@testset "file append" begin
    fin = tpath("xxx4.tgz")
    fout = tpath("xxa")
    pl = pipeline(source(fin), stdout=fout, append=true)
    tl = run(pl)
    @test wait(tl) === nothing
end

@testset "noop optimizations" begin
    pi = ChannelPipe()
    po = ChannelPipe()
    pl = pi → NOOP → po
    tl = run(pl)
    text = "hallo"
    write(pi, text)
    close(Base.pipe_writer(pi))
    @test read(po, String) == text
end
