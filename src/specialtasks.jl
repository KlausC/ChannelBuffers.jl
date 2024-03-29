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

function tarc(dir::AbstractString)
    _tarc(::IO, cout::IO, dir::AbstractString) = Tar.create(dir, cout)
    closure(_tarc, dir)
end

function tarx(dir::AbstractString)
    _tarx(cin::IO, ::IO, dir::AbstractString) = Tar.extract(cin, dir)
    closure(_tarx, dir)
end

function curl(url::AbstractString)
    _curl(::IO, cout::IO, url::AbstractString) = Downloads.download(url, cout)
    closure(_curl, url)
end

function gunzip()
    function _gunzip(cin::IO, cout::IO)
        tc = GzipDecompressorStream(cin)
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        while !eof(tc)
            n = readbytes!(tc, buffer)
            write(cout, view(buffer, 1:n))
        end
    end
    closure(_gunzip)
end

function gzip()
    function _gzip(cin::IO, cout::IO)
        tc = GzipCompressorStream(cin)
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        while !eof(tc)
            n = readbytes!(tc, buffer)
            write(cout, view(buffer, 1:n))
        end
    end
    closure(_gzip)
end

function transcoder(codec::TranscodingStreams.Codec)
    function _transcoder(cin::IO, cout::IO)
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
    closure(_transcoder)
end

function source(src::UIO)
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
    closure(_source, src)
end

function destination(dst::UIO)
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
    closure(_destination, dst)
end

function serializer(obj::Any)
    _serializer(::IO, cout::IO, obj::Any) = Serialization.serialize(cout, obj)
    closure(_serializer, obj)
end

function deserializer()
    _deserializer(cin::IO, ::IO) = Serialization.deserialize(cin)
    closure(_deserializer)
end

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
tarxO() = closure(_tarxO)
