

runtask(f::Function, args...) = schedule(Task(()->f(args...)))

runclose(wio, time) = runtask() do
    sleep(time); close(wio)
end
function runabort(f::Function, wio::ChannelIO, time, args...)
    t = Task(()->f(args...))
    schedule(t, InterruptException(), error=true)
    sleep(time)
    yield()
end
const WTIME = 0.1

function wrcha(bsize=32)
    wio = ChannelIO(:W, bsize);
    rio = ChannelIO(wio.ch);
    wio, rio
end

# test if called function is blocked on reading from channel at least at `t` seconds
function testblock(f, rio, t, args...)
    runcloseabort(rio, t)
    @test_throws InvalidStateException f(args...)
end

@testset "eof" begin
    @test_throws ArgumentError ChannelIO(:X)
    @test (wio = ChannelIO(:W)) !== nothing
    @test ChannelIO(wio.ch) !== nothing
    wio, rio = wrcha()
    @test bytesavailable(rio) == 0
    runclose(wio, WTIME)
    t0 = time()
    @test eof(rio)
    t1 = time()
    @test t1 - t0 >= WTIME
end

@testset "reading one buffer" begin
    input = "0123456789"^3 * "ab"
    ISZ = sizeof(input)
    BSZ = ISZ
    wio, rio = wrcha(BSZ)
    @test bytesavailable(rio) == 0
    @test write(wio, input) == ISZ
    @test bytesavailable(rio) == ISZ
    @test !eof(rio)
    runclose(wio, 0)
    @test read(rio, String) == input
    @test bytesavailable(rio) == 0
end

@testset "reading multiple buffers" begin
    input = "0123456789"^100
    ISZ = sizeof(input)
    BSZ = 32
    wio, rio = wrcha(BSZ)
    @test bytesavailable(rio) == 0
    runtask(wio) do wio
        @test write(wio, input) == ISZ
        close(wio)
    end
    @test bytesavailable(rio) <= BSZ
    @test !eof(rio)
    @test read(rio, String) == input
    @test bytesavailable(rio) == 0
    @test eof(rio)
end

@testset "read on write-only" begin
    wio = ChannelIO(:W)
    @test_throws InvalidStateException read(wio, UInt8)
    @test_throws InvalidStateException peek(wio)
    @test_throws InvalidStateException eof(wio)
    @test_throws InvalidStateException bytesavailable(wio)
    @test flush(wio) === nothing
end

@testset "write on read-only" begin
    rio = ChannelIO(:R)
    @test_throws InvalidStateException write(rio, "hallo")
    @test flush(rio) === nothing
end

"""
@testset "block writing" begin
    input = "0123456789"^10
    ISZ = sizeof(input)
    BSZ = 32
    wio, rio = wrcha(BSZ)
    testblock(wio, 0.1, wio) do wio
        write(wio, input)
    end
end
"""

BSZ = 100
@testset "multiple writes (data size = $ISZ)" for ISZ in [BSZ-1, BSZ+1]
    wio, rio = wrcha(BSZ)
    input = String(rand('A':'z', ISZ))
    runtask(wio) do wio
        for i = 1:2*BSZ
            write(wio, input)
        end
        close(wio)
    end
    output = read(rio, String)
    @test sizeof(output) == sizeof(input) * 2 * BSZ
    @test output == input^(2*BSZ)
end

@testset "bytesio" begin
    bs = 2
    cout = ChannelIO(:W, bs)
    cin = reverseof(cout)
    wrifu() = begin for i = 1:bs; write(cout, UInt8(i)); end; close(cout); end
    t = schedule(Task(wrifu))
    for i = 1:bs
        @test peek(cin) == i
        @test read(cin, UInt8) == i
    end
    @test_throws EOFError peek(cin) 
    wait(t)
end

@testset "resize in readbytes!" begin
    v = zeros(UInt8, 0)
    cin = ChannelIO(); cout = reverseof(cin)
    schedule(Task(() -> begin write(cout, ones(10)); close(cout); end)) |> wait
    n = readbytes!(cin, v, 10)
    @test length(v) == 10
end

@testset "abort takebuffer!" begin
    cin = ChannelIO()
    function waitread()
        read(cin)
    end
    t = schedule(Task(waitread))
    yield()
    close(cin.ch)
    v = fetch(t)
    @test v == UInt8[]
end

@testset "channel pipes" begin
    p = ChannelPipe(10)
    write(p, "test data")
    flush(p)
    @test eof(p) === false
    @test read(p, 9) |> length == 9
    write(p, 123)
    close(p.in)
    b = zeros(UInt8, 100)
    @test readbytes!(p, b, 8) == 8
end
