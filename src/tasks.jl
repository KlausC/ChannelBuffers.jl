
struct BufferTaskDescription{F<:Function,Args<:Tuple}
    f::F
    args::Args
    BufferTaskDescription(f::F, args...) where F = new{F,typeof(args)}(f, args) 
end


const UIO = Union{IO,AbstractString}

struct BufferTask{D<:BufferTaskDescription,In<:UIO,Out<:UIO}
    call::D
    cin::In
    cout::Out
    task::Ref{Task}
    function BufferTask(d::D, cin::In, cout::Out) where {D,In,Out}
        new{D,In,Out}(d, cin, cout, Ref{Task}())
    end
end

struct BufferTaskDList
    list::Vector{<:BufferTaskDescription}
end
struct BufferTaskList
    list::Vector{<:BufferTask}
end

import Base: |
function |(src::BufferTaskDescription, bt::BufferTaskDescription)
    BufferTaskDList([src, bt])
end
function |(src::BufferTaskDList, bt::BufferTaskDescription)
    BufferTaskDList(vcat(src.list, bt))
end
    
function task!(bt::BufferTask)
    function task_function()
        vopen(bt.cin, "r") do cin
            vopen(bt.cout, "w") do cout
                bt.call.f(bt, bt.call.args...)
            end
        end
        nothing
    end
    Task(task_function)
end

function _schedule(s, cin, cout)
    bt = BufferTask(s, cin, cout)
    t = task!(bt)
    bt.task[] = t
    schedule(t)
end

function Base.schedule(btdl::BufferTaskDList)
    n = length(btdl.list)
    tl = Vector{Task}(undef, n)
    s = btdl.list[n]
    cout = devnull
    cin = n == 1 ? devnull : ChannelIO()
    t = _schedule(s, cin, cout)
    tl[n] = t
    for i = n-1:-1:1
        s = btdl.list[i]
        cout = ChannelIO(cin.ch, :W)
        cin = i == 1 ? devnull : ChannelIO()
        t = _schedule(s, cin, cout)
        tl[i] = t
    end
    tl    
end

function vopen(f, io::IO, arg)
    try
        f(io)
    finally
        close(io)
    end
end
function vopen(f, fn::AbstractString, arg)
    open(fn, arg) do io
        f(io)
    end
end
vopen(io::IO, arg) = io
vopen(fn::AbstractString, arg) = open(fn, arg)

using Tar
using Downloads
using CodecZlib

export Source, Tarc, Download
export Gzip, Gunzip
export Destination, Tarx

const DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

function Tarc(dir::AbstractString)
    function _tarc(bt::BufferTask, dir::AbstractString)
        try
            Tar.create(dir, bt.cout)
        finally
            close(bt.cout)
        end
    end
    BufferTaskDescription(_tarc, dir)
end

function Tarx(dir::AbstractString)
    function _tarx(bt::BufferTask, dir::AbstractString)
        try
            Tar.extract(bt.cin, dir)
        finally
            close(bt.cout)
        end
    end
    BufferTaskDescription(_tarx, dir)
end

function Download(url::AbstractString)
    function _download(bt::BufferTask, url::AbstractString)
        try
            Downloads.download(url, bt.cout)
        finally
            close(bt.cout)
        end
    end
    BufferTaskDescription(_download, url)
end

function Gunzip()
    function _gunzip(bt::BufferTask)
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
    BufferTaskDescription(_gunzip)
end

function Gzip()
    function _gzip(bt::BufferTask)
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
    BufferTaskDescription(_gzip)
end

function Source(src::UIO)
    function _source(bt::BufferTask, src::UIO)
        io = vopen(src, "r")
        try
            buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
            while !eof(io)
                n = readbytes!(io, buffer)
                write(bt.cout, view(buffer, 1:n))
            end
        finally
            close(io)
        end
    end
    BufferTaskDescription(_source, src)
end

function Destination(dst::UIO)
    function _destination(bt::BufferTask, dst::UIO)
        buffer = Vector{UInt8}(undef, DEFAULT_READ_BUFFER_SIZE)
        vopen(dst, "w") do io
            while !eof(bt.cin)
                n = readbytes!(bt.cin, buffer)
                write(io, view(buffer, 1:n))
            end
        end
    end
    BufferTaskDescription(_destination, dst)
end


