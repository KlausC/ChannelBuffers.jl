
using ChannelBuffers: BTask, task_function, task_cin, task_cout, task_args, NOOP

dpath(x...) = joinpath(DDIR, x...)
tpath(x...) = joinpath(TDIR, x...)

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
    @test istaskstarted(tl)
    @test istaskdone(tl)
    @test !istaskfailed(tl)
    @test sprint(show, tl) !== nothing
end

@testset "tar -c using pipeline" begin
    file = tpath("xxx0.tgz")
    tar = pipeline(tarc(dpath("xxx")), gzip(), open(file, "w"))
    tl = run(tar)
    close(tar.cout)
    @test length(tl) == 2
    @test wait(tl) === nothing
    @test istaskstarted(tl)
    @test !istaskfailed(tl)
end

@testset "tar -x $dir" for (decoder, dir) in [(gunzip(), "xxx"), (transcoder(GzipDecompressor()), "xxx2")]
    file = tpath("xxx1.tgz")
    tar = tarc(dpath("xxx")) → gzip() → open(file, "w")
    tar |> run |> wait
    close(tar.cout)
    tar = source(file) | decoder | tarx(tpath(dir))
    tl = run(tar)
    @test length(tl) == 3
    @test wait(tl) === nothing
    @test istaskstarted(tl)
    @test !istaskfailed(tl)

    dc = `diff -r "$DDIR/xxx" "$TDIR/$dir"` # check if contents of both dirs are equal
    @test run(dc) !== nothing
end

@testset "copy file" begin
    cpy = source(tpath("xxx.tgz")) | destination(tpath("xxx2.tgz")) → stdout
    tl = run(cpy)
    @test length(tl) == 2
    @test tl[2] isa BTask
    @test wait(tl) === nothing
    @test istaskstarted(tl)
    @test !istaskfailed(tl)
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

@testset "mixed pipline run" begin
    fout = tpath("xxx.txt")
    pl = pipeline(`ls ../src`, ChannelBuffers.noop(), `cat -`, fout)
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

@testset "open task chain for reading" begin
    data = "hello world!"
    io = IOBuffer(data)
    tio = open(noop(), "r", io)
    yield()
    @test tio isa ChannelBuffers.TaskChain
    @test istaskstarted(tio)
    @test !istaskdone(tio)
    @test read(tio, String) == data
    @test istaskstarted(tio)
    @test istaskdone(tio)
    @test !istaskfailed(tio)

    cio = Base.pipe_reader(tio)
    @test isreadable(cio)
    @test !iswritable(cio)
    @test position(cio) == length(data)
end

#=
@testset "open task chain for writing" begin
    io = IOBuffer()
    tio = open(gzip() | noop() | gunzip(), io, write=true)
    for i = 1:5
        println(tio, repeat("abc ", 10))
    end
    close(tio)
    res = String(take!(io))
    @test res == repeat(repeat("abc ", 10) * "\n", 5)
end
=#

@testset "open TaskChain read incomplete" begin
    data = """
    Line 1
    Line 2
    """
    fin = IOBuffer(data)
    s = ""
    open(gzip() | gunzip(), fin) do tio
        s = readline(tio)
        close(tio) # do not read to eof
    end
    @test s == data[1:6]
end
