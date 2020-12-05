using Tar
using Downloads
using TranscodingStreams, CodecZlib
import TranscodingStreams.Codec

export Source, Tarc, Download
export Transcode, Gzip, Gunzip
export Destination, Tarx

const DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

function Tarc(dir::AbstractString)
    function _tarc(cin::IO,cout::IO, dir::AbstractString)
        Tar.create(dir, cout)
    end
    closure(_tarc, dir)
end

function Tarx(dir::AbstractString)
    function _tarx(cin::IO, cout::IO, dir::AbstractString)
        Tar.extract(cin, dir)
    end
    closure(_tarx, dir)
end

function Download(url::AbstractString)
    function _download(cin::IO, cout::IO, url::AbstractString)
        Downloads.download(url, cout)
    end
    closure(_download, url)
end

function Gunzip()
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

function Gzip()
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

function Transcode(codec::Codec)
    function _transcode(cin::IO, cout::IO)
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
    closure(_transcode)
end   

function Source(src::UIO)
    function _source(cin::IO, cout::IO, src::UIO)
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

function Destination(dst::UIO)
    function _destination(cin::IO, cout::IO, dst::UIO)
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
