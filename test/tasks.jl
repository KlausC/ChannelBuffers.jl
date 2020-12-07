
import ChannelBuffers: BTask, task_function, task_cin, task_cout, task_args

const DDIR = abspath(dirname(@__FILE__),"..", "data", "test")
const TDIR = mktempdir(cleanup=false)

println("testing tmpdir is $TDIR")

dpath(x...) = joinpath(DDIR, x...)
tpath(x...) = joinpath(TDIR, x...)

@testset "run individual task" begin
    
file = dpath("xxx", "a")
    io = IOBuffer()
    run(source(file), stdout = io) |> wait
    @test String(take!(io)) == "This is a small test data file"
end

@testset "serialize and deserialize" begin
    obj = ["hans", 42]
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
    tar = pipeline(tarc(dpath("xxx")), gzip(); stdout = open(file, "w"))
    tl = run(tar)
    close(tar.cout)
    @test length(tl) == 2
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl))
    @test !any(istaskfailed.(tl))
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
    @test all(istaskstarted.(tl))
    @test !any(istaskfailed.(tl))

    dc = `diff -r "$DDIR/xxx" "$TDIR/$dir"` # check if contents of both dirs are equal
    @test run(dc) !== nothing
end

@testset "copy file" begin
    cpy = source(tpath("xxx.tgz")) | destination(tpath("xxx2.tgz"))
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
    open(fin) do cin
        open(fout, "w") do cout
            (cin → gunzip() | gzip() → cout) |> run |> wait
        end
    end
    fc = `diff "$fin" "$fout"`
    @test run(fc) !== nothing
end