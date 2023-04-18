
abstract type AbstractChannelIO <: IO end

import Base: open, close, readbytes!, Redirectable


const R = :R
const W = :W

"""
    ChannelIO

Is read and written like an `IOStream`. If buffers are empty/full transport them using
channels allowing parallel task pipelining.
"""
mutable struct ChannelIO{T<:AbstractVector{UInt8}} <: AbstractChannelIO
    ch::Channel{T}
    rw::Symbol # R readonly W writeonly
    buffer::T
    bufsize::Int
    woffset::Int
    roffset::Int
    eofpending::Bool
    position::Int # offset from stream start of first byte in buffer
    mark::Int
    function ChannelIO(ch::Channel, rw::Symbol, buffer::V, bufsize::Integer) where V
        bufsize > 0 || throw(ArgumentError("minimal buffer size is $bufsize"))
        rw == R || rw == W || throw(ArgumentError("read/write must be $R or $W"))
        new{V}(ch, rw, buffer, bufsize, 0, 0, false, 0, -1)
    end
end

const DEFAULT_BUFFER_SIZE = 8192
const DEFAULT_CHANNEL_LENGTH = 1

struct ChannelPipe{T} <: AbstractPipe
    in::ChannelIO{T}
    out::ChannelIO{T}
    function ChannelPipe(cin::ChannelIO{T}, cout::ChannelIO{T}) where T
        cin.ch === cout.ch || throw(ArgumentError("cin and cout must share same Channel"))
        iswritable(cin) || throw(ArgumentError("cin must be writable"))
        isreadable(cout) || throw(ArgumentError("cout must be readable"))
        new{T}(cin, cout)
    end
end

function ChannelPipe(bufsize::Integer=DEFAULT_BUFFER_SIZE)
    out = ChannelIO(bufsize)
    in = reverseof(out)
    ChannelPipe(in, out)
end

function ChannelPipe(cio::ChannelIO{T}) where T
    ciow = reverseof(cio)
    if isreadable(ciow)
        ciow, cio = cio, ciow
    end
    ChannelPipe(ciow, cio)
end

const AllChannelIO = Union{ChannelIO,ChannelPipe}

function ChannelIO(ch::Channel, rw::Symbol=R, bufsize::Integer=DEFAULT_BUFFER_SIZE)
    ChannelIO(ch, rw, zeros(UInt8, 0), bufsize)
end

function ChannelIO(rw::Symbol, bufsize::Integer=DEFAULT_BUFFER_SIZE)
    T = Vector{UInt8}
    ch = Channel{T}(DEFAULT_CHANNEL_LENGTH)
    ChannelIO(ch, rw, bufsize)
end

function ChannelIO(bufsize::Integer=DEFAULT_BUFFER_SIZE)
    ChannelIO(R, bufsize)
end

# create ChannelIO with same channel and reverse read/write indicator
function reverseof(cio::ChannelIO)
    rev = cio.rw == R ? W : R
    ChannelIO(cio.ch, rev, cio.bufsize)
end

function isopen(cio::ChannelIO)
    isopen(cio.ch) || isready(cio.ch)
end

@noinline function throw_inv(cio::ChannelIO)
    text = iswritable(cio) && !isopenwritable(cio) ? "closed" : "$(cio.rw)-only"
    throw(InvalidStateException("$(cio.rw)-channel is $text", cio.rw))
end

@noinline function throw_closed(cio::ChannelIO)
    throw(InvalidStateException("channel is closed.", cio.rw))
end

function vput!(cio::ChannelIO{T}, b::T) where T
    put!(cio.ch, b)
    isopen(cio.ch) || throw_closed(cio)
    b
end

function write(cio::ChannelIO, byte::UInt8)
    isopenwritable(cio) || throw_inv(cio)
    n = length(cio.buffer)
    cio.woffset += 1
    if cio.woffset > n
        resize!(cio.buffer, max(cio.woffset, cio.bufsize))
    end
    cio.buffer[cio.woffset] = byte
    if cio.woffset >= cio.bufsize
        _flush(cio, false)
    end
end

function unsafe_write(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    isopenwritable(cio) || throw_inv(cio)
    k = cio.woffset
    if length(cio.buffer) < cio.bufsize
        resize!(cio.buffer, cio.bufsize)
    end
    bufsize = length(cio.buffer)
    d = pointer(cio.buffer, k+1)
    s = pp
    n = Int(nn)
    while n + k >= bufsize
        i = bufsize - k
        unsafe_copyto!(d, s, i)
        vput!(cio, cio.buffer)
        cio.position += bufsize
        newbuffer!(cio)
        bufsize = length(cio.buffer)
        d = pointer(cio.buffer, 1)
        k = 0
        n -= i
        s += i
    end
    if n > 0
        unsafe_copyto!(d, s, n)
        cio.woffset += n
    end
    Int(nn)
end

function flush(cio::ChannelIO)
    isopenwritable(cio) || return nothing
    _flush(cio, false)
end

function _flush(cio::ChannelIO, close::Bool)
    cio.woffset == 0 && !close && return
    resize!(cio.buffer, cio.woffset)
    try
        vput!(cio, cio.buffer)
    catch
        !close && rethrow()
    finally
        cio.position += cio.woffset
        newbuffer!(cio)
    end
    nothing
end

function destroy(cio::ChannelIO)
    isreadable(cio) || return nothing
    _destroy(cio)
end

function _destroy(cio::ChannelIO)
    lock(ch)
    try
        while isready(ch)
            take!(ch)
        end
    catch
        # ignore
    finally
        cio.eofpending = true
        close(ch)
        unlock(ch)
    end
    nothing
end

function close(cio::ChannelIO)
    isopen(cio.ch) || return
    try
        if isreadable(cio)
            _destroy(cio)
        else
            _flush(cio, true)
        end
    catch
        # ignore error during close
    finally
        close(cio.ch)
    end
    nothing
end

function peek(cio::ChannelIO)
    isreadable(cio) || throw_inv(cio)
    eof(cio) && throw(EOFError())
    cio.buffer[cio.roffset + 1]
end
function read(cio::ChannelIO, ::Type{UInt8})
    isreadable(cio) || throw_inv(cio)
    eof(cio) && throw(EOFError())
    c = cio.buffer[cio.roffset += 1]
end

function bytesavailable(cio::ChannelIO)
    isreadable(cio) || throw_inv(cio)
    n = cio.woffset - cio.roffset
    if n > 0
        return Int(n)
    else
        cio.eofpending && return 0
        cio.woffset = 0
        cio.roffset = length(cio.buffer)
    end
    if isready(cio.ch)
        takebuffer!(cio)
    end
    Int(cio.woffset)
end

function eof(cio::ChannelIO)
    isreadable(cio) || throw_inv(cio)
    cio.roffset < length(cio.buffer) && return false
    takebuffer!(cio)
    return cio.eofpending && cio.roffset >= length(cio.buffer)
end

function readbytes!(cio::ChannelIO, b::Vector{UInt8}, nb=length(b); all::Bool=true)
    s = bytesavailable(cio)
    n = min(s, nb)
    if n > length(b)
        resize!(b, n)
    end
    r = unsafe_read(cio, pointer(b, 1), n)
    if !all || r >= nb
        return Int(r)
    end
    while r < nb && !eof(cio)
        n = min(nb, r + DEFAULT_BUFFER_SIZE)
        if n > length(b)
            resize!(b, n)
        end
        k = unsafe_read(cio, pointer(b, r + 1), n - r)
        r += k
    end
    return Int(r)
end

function unsafe_read(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    isreadable(cio) || throw_inv(cio)
    p = pp
    n = Int(nn)
    while n > 0 && !eof(cio)
        k = cio.woffset - cio.roffset
        s = pointer(cio.buffer, cio.roffset + 1)
        i = min(k, n)
        unsafe_copyto!(p, s, i)
        k -= i
        n -= i
        p += i
        cio.roffset += i
    end
    Int(nn - n)
end

function newbuffer!(cio::ChannelIO)
    #Vector{UInt8}(undef, cio.bufsize)
    cio.buffer = zeros(UInt8, cio.bufsize)
    cio.roffset = 0
    cio.woffset = 0
end

function takebuffer!(cio::ChannelIO)
    bufsize = length(cio.buffer)
    if isopen(cio.ch) || isready(cio.ch)
        try
            buffer = take!(cio.ch)
        catch
            buffer = UInt8[]
        end
    else
        buffer = UInt8[]
    end
    cio.roffset = 0
    cio.position += bufsize
    cio.buffer = buffer
    cio.woffset = length(cio.buffer)
    cio.eofpending |= cio.woffset == 0
    cio.woffset
end

isreadable(cio::ChannelIO) = cio.rw == R
iswritable(cio::ChannelIO) = !isreadable(cio)
isopenwritable(cio::ChannelIO) = iswritable(cio) && isopen(cio.ch)

function position(cio::ChannelIO)
    cio.position + (iswritable(cio) ? cio.woffset : cio.roffset)
end

function seek(cio::ChannelIO, p::Integer)
    n = length(cio.buffer)
    seek_start = cio.position
    seek_end = cio.position + n
    pp = p - position(cio)
    if seek_start <= p < seek_end
        if isreadable(cio)
            cio.roffset += pp
        else
            cio.woffset += pp
        end
    else
        boundaries = "$(seek_start) <= $p < $(seek_end)"
        throw(ArgumentError("cannot seek beyond positions ($boundaries)"))
    end
    cio
end

function show(io::IO, cio::ChannelIO)
    print(io, "ChannelIO(", cio.rw, ", ")
    if isreadable(cio)
        print(io, channel_length(cio), " → ", buffer_length(cio))
    else
        print(io, buffer_length(cio), " → ", channel_length(cio))
    end
    print(io, ", position ", position(cio), ", ", channel_state(cio), ")")
end

function show(io::IO, cio::ChannelPipe)
    print(io, "ChannelPipe(", )
    print(io, buffer_length(cio.in), " → ", channel_length(cio), " → ")
    print(io, buffer_length(cio.out), ", ", channel_state(cio), ")")
end

channel_length(ch::Channel) = sum(length.(ch.data))
channel_length(cio::ChannelIO) = channel_length(cio.ch)
channel_length(cp::ChannelPipe) = channel_length(cp.in)
channel_state(ch::Channel) = ch.state
function channel_state(cio::ChannelIO)
    cs = channel_state(cio.ch)
    if cs == :closed
        cs = isreadable(cio) && eof(cio) ? :eof : cs
    end
    cs
end
channel_state(cio::ChannelPipe) = channel_state(cio.out)

function buffer_length(cio::ChannelIO)
    isreadable(cio) ? length(cio.buffer) - cio.roffset : cio.woffset
end

write(io::ChannelPipe, x::UInt8) = write(io.in, x)
unsafe_write(io::ChannelPipe, p::Ptr{UInt8}, n::UInt) = unsafe_write(io.in, p, n)
flush(io::ChannelPipe) = flush(io.in)
unsafe_read(io::ChannelPipe, p::Ptr{UInt8}, n::UInt) = unsafe_read(io.out, p, n)
eof(io::ChannelPipe) = eof(io.out)
bytesavailable(io::ChannelPipe) = bytesavailable(io.out)
peek(io::ChannelPipe) = peek(io.out)
readbytes!(io::ChannelPipe, p::AbstractVector{UInt8}, n=length(p)) = readbytes!(io.out, p, n)
read(io::ChannelPipe, T::Type{UInt8}) = read(io.out, T)
pipe_reader(io::ChannelPipe) = io.out
pipe_writer(io::ChannelPipe) = io.in
close(io::ChannelPipe) = begin close(io.in); close(io.out) end

#= support debugging
using Infiltrator
struct IOWrapper{T<:IO} <: IO
    io::T
end
export IOWrapper

eof(io::IOWrapper) = eof(io.io)
read(io::IOWrapper, args...; kwargs...) = read(io.io, args...; kwargs...)
write(io::IOWrapper, x::UInt8) = write(io.io, x)
take!(s::IOWrapper) = take!(s.io)
function close(s::IOWrapper)
    @infiltrate
    close(s.io)
end
=#
