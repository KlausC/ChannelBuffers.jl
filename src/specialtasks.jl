using Tar
using Downloads
using TranscodingStreams, CodecZlib
using Serialization

export source, destination
export curl
export transcoder, gunzip, gzip
export tarc, tarx, tarxO
export serializer, deserializer

const DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

# noop() task - copy cin to cout
noop() = closure(_noop)
function _noop(cin::IO, cout::IO)
    while !eof(cin)
        write(cout, read(cin, DEFAULT_READ_BUFFER_SIZE))
    end
end

export spy
spy() = closure(_spy)
function _spy(cin::IO, cout::IO)
    _noop(cin, cout)
    println(cout, "worker $(myid()) thread $(Threads.threadid()) task $(current_task())")
    println(cout, "from $cin to $cout")
end

tarc(dir::AbstractString) = closure(_tarc, dir)
_tarc(::IO, cout::IO, dir::AbstractString) = Tar.create(dir, cout)

tarx(dir::AbstractString) = closure(_tarx, dir)
_tarx(cin::IO, ::IO, dir::AbstractString) = Tar.extract(cin, dir)

curl(url::AbstractString) = closure(_curl, url)
_curl(::IO, cout::IO, url::AbstractString) = Downloads.download(url, cout)

gunzip() = closure(_gunzip)
function _gunzip(cin::IO, cout::IO)
    tc = GzipDecompressorStream(cin)
    buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
    while !eof(tc)
        n = readbytes!(tc, buffer)
        write(cout, view(buffer, 1:n))
    end
end

gzip() = closure(_gzip)
function _gzip(cin::IO, cout::IO)
    tc = GzipCompressorStream(cin)
    buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
    while !eof(tc)
        n = readbytes!(tc, buffer)
        write(cout, view(buffer, 1:n))
    end
end

transcoder(codec::TranscodingStreams.Codec) = closure(_transcoder, codec)
function _transcoder(cin::IO, cout::IO, codec::TranscodingStreams.Codec)
    buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE * 10)
    TranscodingStreams.initialize(codec)
    while !eof(cin)
        n = readbytes!(cin, buffer)
        r = transcode(codec, buffer[1:n])
        write(cout, r)
    end
    flush(cout)
    TranscodingStreams.finalize(codec)
end

source(src::UIO) = closure(_source, src)
function _source(::IO, cout::IO, src::UIO)
    io = src isa IO ? src : io = open(src, "r")
    try
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        while !eof(io)
            n = readbytes!(io, buffer)
            write(cout, view(buffer, 1:n))
        end
    finally
        src isa IO || close(io)
    end
end

destination(dst::UIO) = closure(_destination, dst)
function _destination(cin::IO, ::IO, dst::UIO)
    buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
    io = dst isa IO ? dst : open(dst, "w")
    try
        while !eof(cin)
            n = readbytes!(cin, buffer)
            write(io, view(buffer, 1:n))
        end
    finally
        dst isa IO || close(io)
    end
end

serializer(obj::Any) = closure(_serializer, obj)
_serializer(::IO, cout::IO, obj::Any) = Serialization.serialize(cout, obj)

deserializer() = closure(_deserializer)
_deserializer(cin::IO, ::IO) = Serialization.deserialize(cin)

tarxO() = closure(_tarxO)
const TARHDR = 512
function _tarxO(in::IO, out::IO)
    sz = 0
    while !eof(in)
        b = read(in, TARHDR)
        if sz <= 0
            sz = parse(Int, String(b[125:135]), base=8)
            String(b[258:263]) != "ustar\0" && throw(Error("no tar file"))
        else
            rest = min(TARHDR, sz)
            write(out, b[1:rest])
            sz -= rest
        end
    end
end
