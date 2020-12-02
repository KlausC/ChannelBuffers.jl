
abstract type BufferTask end

mutable struct SourceBufferTask{F<:Function,Args,Out<:IO} <: BufferTask
    f::F
    args::Args
    cout::Out
    task::Task
    function SourceBufferTask(ff, args...)
        cout = ChannelIO()
        new{typeof(ff),typeof(args),typeof(cout)}(ff, args, cout)
    end
end

mutable struct TransBufferTask{F<:Function,Args,In<:IO,Out<:IO} <: BufferTask
    f::F
    args::Args
    cout::Out
    cin::In
    task::Task
    function TransBufferTask(ff, args...)
        cout = ChannelIO()
        new{typeof(ff),typeof(args),typeof(cout),typeof(cout)}(ff, args, cout)
    end
end

mutable struct DestinationBufferTask{F<:Function,Args,In<:IO} <: BufferTask
    f::F
    args::Args
    cin::In
    task::Task
    function DestinationBufferTask(ff, args...)
        cin = ChannelIO()
        new{typeof(ff),typeof(args),typeof(cin)}(ff, args)
    end
end

struct BufferTaskList
    list::Vector{<:BufferTask}
end

import Base: |
function |(src::SourceBufferTask, bt::Union{DestinationBufferTask,TransBufferTask})
    bt.cin = ChannelIO(src.cout.ch)
    BufferTaskList([src, bt])
end
function |(src::BufferTaskList, bt::Union{DestinationBufferTask,TransBufferTask})
    bt.cin = ChannelIO(last(src.list).cout.ch)
    BufferTaskList(vcat(src.list, bt))
end
    
function Base.schedule(btl::BufferTaskList)
    t = missing
    for bt in btl.list
        t = Task(() -> bt.f(bt, bt.args...))
        bt.task = t
        schedule(t)
    end
    t 
end

using Tar
using Downloads
using CodecZlib

export Source, Tarc, Download
export Gzip, Gunzip
export Destination, Tarx

const DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

function Tarc(dir::AbstractString)
    function ff(bt::BufferTask, dir::AbstractString)
        try
            Tar.create(dir, bt.cout)
        finally
            close(bt.cout)
        end
    end
    SourceBufferTask(ff, dir)
end

function Tarx(dir::AbstractString)
    function ff(bt::BufferTask, dir::AbstractString)
        try
            Tar.extract(bt.cin, dir)
        finally
            close(bt.cout)
        end
    end
    DestinationBufferTask(ff, dir)
end

function Download(url::AbstractString)
    function ff(bt::BufferTask, url::AbstractString)
        try
        Downloads.download(url, bt.cout)
        finally
            close(bt.cout)
        end
    end
    SourceBufferTask(ff, url)
end

function Gunzip()
    function ff(bt::BufferTask)
        try
        tc = GzipDecompressorStream(bt.cin)
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        while !eof(tc)
            readbytes!(tc, buffer)
            write(bt.cout, buffer)
        end
        finally
            close(bt.cout)
        end
    end
    TransBufferTask(ff)
end

function Gzip()
    function ff(;bt::BufferTask)
        try
            tc = GzipCompressorStream(bt.cin)
            buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
            while !eof(tc)
                n = readbytes!(tc, buffer)
                write(bt.cout, view(buffer, 1:n))
            end
        finally
            close(bt.cout)
        end
    end
    TransBufferTask(ff)
end

function Source(src::Union{IO,AbstractString})
    function ff(bt::BufferTask, src::Union{IO,AbstractString})
            io = open(src, "r")
        try
            buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
                while !eof(io)
                    n = readbytes!(io, buffer)
                    write(bt.cout, view(buffer, 1:n))
                end
        finally
            close(io)
            close(bt.cout)
        end
    end
    SourceBufferTask(ff, src)
end

function Destination(dst::Union{IO,AbstractString})
    function ff(bt::BufferTask, dst::Union{IO,AbstractString})
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        open(dst, "w") do io
            while !eof(bt.cin)
                n = readbytes!(bt.cin, buffer)
                write(io, view(buffer, 1:n))
            end
        end
    end
    DestinationBufferTask(ff, dst)
end


