
const DDIR = abspath(dirname(@__FILE__),"..", "data", "test")
const TDIR = mktempdir(cleanup=false)

println("testing tmpdir is $TDIR")

@testset "tar -c" begin
    tar = tarc(joinpath(DDIR, "xxx")) | gzip() | destination(joinpath(TDIR, "xxx.tgz"))
    tl = run(tar)
    @test length(tl) == 3
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl.list))
    @test !any(istaskfailed.(tl.list))
end

@testset "tar -c using pipeline" begin
tar = pipeline(tarc(joinpath(DDIR, "xxx")), gzip(); stdout = open(joinpath(TDIR, "xxx.tgz"), "w"))
tl = run(tar)
@test length(tl) == 2
@test wait(tl) === nothing
@test all(istaskstarted.(tl.list))
@test !any(istaskfailed.(tl.list))
end

@testset "tar -x" begin
    tar = source(joinpath(TDIR, "xxx.tgz")) | transcoder(GzipDecompressor()) | tarx(joinpath(TDIR, "xxx"))
    tl = run(tar)
    @test length(tl) == 3
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl.list))
    @test !any(istaskfailed.(tl.list))

    dc = `diff -r "$DDIR/xxx" "$TDIR/xxx"` # check if contents of both dirs are equal
    @test run(dc) !== nothing
end

@testset "copy file" begin
    cpy = source(joinpath(TDIR, "xxx.tgz")) | destination(joinpath(TDIR, "xxx2.tgz"))
    tl = run(cpy)
    @test length(tl) == 2
    @test wait(tl) === nothing
    @test all(istaskstarted.(tl.list))
    @test !any(istaskfailed.(tl.list))
    fc = `diff -r "$TDIR/xxx.tgz" "$TDIR/xxx2.tgz"` # check if contents of both dirs are equal
    @test run(fc) !== nothing
end

@testset "downloads and output redirection" begin
    open(joinpath(TDIR, "xxx3.tgz"), "w") do io
        wait(run(curl("file://" * TDIR * "/xxx.tgz"), stdout=io))   
    end
    fc = `diff -r "$TDIR/xxx.tgz" "$TDIR/xxx3.tgz"`
    @test run(fc) !== nothing
end

@testset "destination and input redirection" begin
    open(joinpath(TDIR, "xxx.tgz"), "r") do io
        wait(run(destination(joinpath(TDIR, "xxx4.tgz")) < io))   
    end
    fc = `diff -r "$TDIR/xxx.tgz" "$TDIR/xxx4.tgz"`
    @test run(fc) !== nothing
end