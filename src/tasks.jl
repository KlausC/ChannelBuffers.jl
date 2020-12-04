
struct BClosure{F<:Function,Args<:Tuple}
    f::F
    args::Args
    BClosure(f::F, args...) where F = new{F,typeof(args)}(f, args) 
end

const UIO = Union{IO,AbstractString}

struct BClosureList
    list::Vector{<:BClosure}
end

import Base: |
function |(src::BClosure, btd::BClosure)
    BClosureList([src, btd])
end
function |(src::BClosureList, btd::BClosure)
    BClosureList(vcat(src.list, btd))
end
    
function task!(btd::BClosure, cin, cout)
    function task_function()
        try
            btd.f(cin, cout, btd.args...)
        finally
            cout isa ChannelIO && close(cout)
        end
    end
    Task(task_function)
end

function _schedule(s, cin, cout)
    t = task!(s, cin, cout)
    schedule(t)
end

function Base.schedule(btdl::BClosureList; stdin=stdin, stdout=stdout)
    n = length(btdl.list)
    tl = Vector{Task}(undef, n)
    s = btdl.list[n]
    cout = stdout
    cin = n == 1 ? stdin : ChannelIO()
    t = _schedule(s, cin, cout)
    tl[n] = t
    for i = n-1:-1:1
        s = btdl.list[i]
        cout = ChannelIO(cin.ch, :W)
        cin = i == 1 ? stdin : ChannelIO()
        t = _schedule(s, cin, cout)
        tl[i] = t
    end
    tl    
end

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
    BClosure(_tarc, dir)
end

function Tarx(dir::AbstractString)
    function _tarx(cio::IO, cout::IO, dir::AbstractString)
        Tar.extract(cin, dir)
    end
    BClosure(_tarx, dir)
end

function Download(url::AbstractString)
    function _download(cin::IO, cout::IO, url::AbstractString)
        Downloads.download(url, cout)
    end
    BClosure(_download, url)
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
    BClosure(_gunzip)
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
    BClosure(_gzip)
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
    BClosure(_transcode)
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
    BClosure(_source, src)
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
    BClosure(_destination, dst)
end
