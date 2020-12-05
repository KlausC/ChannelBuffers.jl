
"""
    BClosure(f::function, args)

Store function and arguments. The signature of the function must
be like `f(cin::IO, cout::IO, args...)`.
"""
struct BClosure{F<:Function,Args<:Tuple}
    f::F
    args::Args
end

# used for output redirection
const UIO = Union{IO,AbstractString}

# List of BClosure objects an io redirections
struct BClosureList{In,Out}
    list::Vector{<:BClosure}
    cin::In
    cout::Out
    BClosureList(list, cin::In, cout::Out) where {In,Out} = new{In,Out}(list, cin, cout)
end
BClosureList(list) = BClosureList(list, stdin, stdout)

# List of tasks - output of `schedule`
struct BTaskList
    list::Vector{<:Task}
end

import Base: |, <, >
|(src::BClosureList, btd::BClosure) = BClosureList(vcat(src.list, btd))
|(src::BClosure, btd::BClosure) = BClosureList([src, btd])

<(list::BClosureList, cin::IO) = BClosureList(list.list, cin, list.cout)
<(list::BClosure, cin::IO) = BClosureList([list], cin, stdout)

>(list::BClosureList, cout::IO) = BClosureList(list.list, list.cin, cout)
>(list::BClosure, cout::IO) = BClosureList(list.list, stdin, cout)

function Base.pipeline(src::BClosure, other::BClosure...; stdin=stdin, stdout=stdout)
    BClosureList([src; other...], stdin, stdout)
end

"""
    wait(tl::BTaskList)

Wait for the last task in the list to finish.
"""
function Base.wait(tv::BTaskList)
    if length(tv.list) > 0
        wait(last(tv.list))
    end
end
Base.length(tv::BTaskList) = length(tv.list)
Base.getindex(tv::BTaskList, i) = getindex(tv.list, i)
Base.show(io::IO, m::MIME"text/plain", tv::BTaskList) = show(io, m, tv.list)

function _schedule(btd::BClosure, cin, cout)
    function task_function()
        try
            btd.f(cin, cout, btd.args...)
        finally
            cout isa ChannelIO && close(cout)
        end
    end
    if Threads.nthreads() <= 1 || Threads.threadid() != 1
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end

"""
    getcode

Access the argumentless function provided to the task
"""
getcode(t::Task) = t.code.task_function
getcin(t::Task) = getcode(t).cin
getcout(t::Task) = getcode(t).cout

"""
    run(BClosure; stdin=stdin, stdout=stdout)

Start parallel task redirecting stdin and stdout
"""
function Base.run(bt::BClosure; stdin=stdin, stdout=stdout)
    run(BClosureList([bt], stdin, stdout))
end

"""
    run(::BClosureList)

Start all parallel tasks defined in list, io redirection defaults are defined in the list
"""
function Base.run(btdl::BClosureList)
    stdin = btdl.cin
    stdout = btdl.cout
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
    BTaskList(tl)    
end

using Tar
using Downloads
using TranscodingStreams, CodecZlib
import TranscodingStreams.Codec

export Source, Tarc, Download
export Transcode, Gzip, Gunzip
export Destination, Tarx

const DEFAULT_READ_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

"""
    closure(f::Function, args...)

Generate a `BClosure` object, which can be used to be started in parallel.
The function `f` must have the signature `f(cin::IO, cout::IO [, args...])`.
It is wrapped in an argumentless closure to be used in a `Task` definition.
"""
function closure(f::Function, args...)
    BClosure(f, args)
end

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
