
using ChannelBuffers: BTask, task_function, task_cin, task_cout, task_args, NOOP

@testset "BTask{T,Process}" begin
    pl = ChannelBuffers.BClosureList([`false`])
    tl = run(pl, wait=false)
    @test fetch(tl) == 1
    @test istaskstarted(tl)
    @test istaskdone(tl)
    @test istaskfailed(tl)
end

@testset "run individual task" begin
    file = tpath("tfile")
    text = "This is a small test data file"
    open(file, "w") do io
        write(io, text)
    end
    io = IOContext(IOBuffer())
    run(source(file), stdout = io)
    @test String(take!(io.io)) == text
end

@testset "serialize and deserialize" begin
    obj = ["hans", 92]
    tl = run(serializer(obj) | deserializer())
    @test fetch(tl) == obj
    tl = run(serializer(obj) | gzip() | deserializer(); wait = false)
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
    tl = run(cpy, wait=false)
    @test length(tl) == 2
    @test tl[2] isa BTask
    @test wait(tl) === nothing
    @test istaskstarted(tl)
    @test !istaskfailed(tl)
    fc = `diff -r "$TDIR/xxx.tgz" "$TDIR/xxx2.tgz"` # check if contents of both dirs are equal
    @test run(fc, wait=false) !== nothing
    bt = tl[2]
    @test task_cin(bt) isa IO
    @test task_cout(bt) isa IO
    @test task_function(bt) isa Function
    @test task_args(bt) isa Tuple{String}
end

@testset "downloads and output redirection" begin
    open(tpath("xxx3.tgz"), "w") do io
        run(curl("file://" * TDIR * "/xxx.tgz") → io)
    end
    fc = `diff "$TDIR/xxx.tgz" "$TDIR/xxx3.tgz"`
    @test run(fc) !== nothing
end

@testset "destination and input redirection" begin
    open(tpath("xxx.tgz"), "r") do io
        (io → destination(tpath("xxx4.tgz"))) |> run
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
    tl = run(pl, wait = false)
    @test wait(tl) === nothing
    @test run(pipeline(`ls ../src`, `cmp - $fout`)) !== nothing
end

@testset "file append" begin
    fin = tpath("xxx4.tgz")
    fout = tpath("xxa")
    pl = pipeline(source(fin), stdout=fout, append=true)
    tl = run(pl, wait=false)
    @test wait(tl) === nothing
end

@testset "noop optimizations" begin
    pi = ChannelPipe()
    po = ChannelPipe()
    pl = pi → NOOP → po
    tl = run(pl, wait=false)
    text = "hallo"
    write(pi, text)
    close(Base.pipe_writer(pi))
    @test read(po, String) == text
end

@testset "open task chain for reading" begin
    data = "hello world!" ^ 10000
    io = IOBuffer(data)
    tio = open(noop(), io; read=true)
    yield()
    @test tio isa ChannelBuffers.TaskChain
    @test istaskstarted(tio)
    @test !istaskdone(tio)
    @test read(tio, String) == data
    wait(tio)
    @test istaskstarted(tio)
    @test istaskdone(tio)
    @test !istaskfailed(tio)

    cio = Base.pipe_reader(tio)
    @test isreadable(cio)
    @test !iswritable(cio)
    @test position(cio) == length(data)
end

@testset "open BClosure" for pl in ( noop(), gzip() | gunzip())
    @test_throws ArgumentError open(pl, "r+", stdout)
    tl = open(pl, "r+")
    @test tl.in isa ChannelBuffers.ChannelIO
    @test tl.out isa ChannelBuffers.ChannelIO
    close(Base.pipe_writer(tl))
    wait(tl)
    tl = open(pl, "w+")
    @test tl.in isa ChannelBuffers.ChannelIO
    @test tl.out isa ChannelBuffers.ChannelIO
    close(tl)
    wait(tl)
    tl = open(pl, "r", devnull)
    @test tl.in === devnull
    @test tl.out isa ChannelBuffers.ChannelIO
    close(tl)
    wait(tl)
    io = IOBuffer()
    open(pl, "w", io) do tl
        @test tl.in isa ChannelBuffers.ChannelIO
        @test tl.out === devnull
        close(Base.pipe_writer(tl))
        @test fetch(tl) === nothing
    end
end

@testset "open task chain for writing" begin
    io = IOBuffer()
    save_tio = nothing
    open(gzip() | noop() | gunzip(), io, write=true) do tio
        for i = 1:5
            println(tio, repeat("abc ", 10))
        end
        @test !istaskdone(tio)
        save_tio = tio
    end
    wait(save_tio)
    @test istaskdone(save_tio)
    res = String(take!(io))
    @test res == repeat(repeat("abc ", 10) * "\n", 5)
end

@testset "open TaskChain read incomplete" begin
    dir = dpath("xxx")
    data = "module ChannelBuffers"
    s = ""
    open(tarc(dir) | tarxO()) do tio
        while !eof(tio)
            s = readline(tio)
            s == data && break
        end
        close(tio) # do not read to eof
    end
    @test s == data
end

@testset "don't vclose TTY" begin
    @test ChannelBuffers.vclose(stderr, true) === nothing
end

@testset "kill TaskChain" begin
    cin = ChannelPipe()
    cout = ChannelPipe()
    tl = run( cin → gzip() | gunzip() → cout, wait=false)
    write(tl, "hallo")
    while !istaskstarted(tl)
        yield()
    end
    @test !istaskdone(tl)
    kill(tl)
    @test_throws TaskFailedException wait(tl)
    @test istaskdone(tl)
    @test istaskfailed(tl)

    tl = run(`sleep 10` | noop(), wait=false)
    while !istaskstarted(tl)
        yield()
    end
    @test istaskstarted(tl[1])
    @test !istaskdone(tl)
    kill(tl)
    wait(tl)
    @test istaskdone(tl)
    @test kill(tl) === nothing
end
