
abstract type AbstractChannelIO <: IO end

import Base: open, close, readbytes!, Redirectable
using Infiltrator

const R = :R
const W = :W

"""
    ChannelIO

Is read and written like an `IOStream`. If buffers are empty/full transport them using
channels allowing parallel task pipelining.
"""
mutable struct ChannelIO{RW,T<:AbstractVector{UInt8}} <: AbstractChannelIO
    ch::Channel{T}
    buffer::T
    bufsize::Int
    offset::Int
    eofpending::Bool
    position::Int # offset from stream start of first byte in buffer
    mark::Int
    function ChannelIO(ch::Channel, rw::Symbol, buffer::V, bufsize::Integer) where V
        bufsize > 0 || throw(ArgumentError("minimal buffer size is $bufsize"))
        rw == R || rw == W || throw(ArgumentError("read/write must be $R or $W"))
        new{rw,V}(ch, buffer, bufsize, 0, false, 0, -1)
    end
end

const DEFAULT_BUFFER_SIZE = 8192
const DEFAULT_CHANNEL_LENGTH = 1

struct ChannelPipe{T} <: AbstractPipe
    in::ChannelIO{W,T}
    out::ChannelIO{R,T}
    function ChannelPipe(cin::ChannelIO{W,T}, cout::ChannelIO{R,T}) where T
        cin.ch === cout.ch || throw(ArgumentError("cin and cout must share same Channel"))
        new{T}(cin, cout)
    end
end

function ChannelPipe(bufsize::Integer=DEFAULT_BUFFER_SIZE)
    out = ChannelIO(bufsize)
    in = reverseof(out)
    ChannelPipe(in, out)
end

ChannelPipe(cio::ChannelIO{R}) = ChannelPipe(reverseof(cio), cio)
ChannelPipe(cio::ChannelIO{W}) = ChannelPipe(cio, reverseof(cio))

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
    rev = isreadable(cio) ? W : R
    ChannelIO(cio.ch, rev, cio.bufsize)
end

isopen(cio::ChannelIO{R}) = isopen(cio.ch) || isready(cio.ch)
isopen(cio::ChannelIO{W}) = isopen(cio.ch)

@noinline function throw_invalid(cio::ChannelIO{RW}) where RW
    rw = RW
    text = iswritable(cio) && !isopen(cio) ? "closed" : "$(rw)-only"
    throw(InvalidStateException("$(rw)-channel is $text", rw))
end
@noinline function throw_closed(::ChannelIO{RW}) where RW
    throw(InvalidStateException("channel has been closed.", RW))
end
@noinline function throw_wrong_mode(::ChannelIO{RW}) where RW
    throw(InvalidStateException("channel has wrong mode.", RW))
end

function vput!(cio::ChannelIO{W,T}, b::T) where T
    put!(cio.ch, b)
    isopen(cio.ch) || throw_closed(cio)
    b
end

write(cio::ChannelIO{R}, ::UInt8) = throw_wrong_mode(cio)
function write(cio::ChannelIO{W}, byte::UInt8)
    isopen(cio) || throw_invalid(cio)
    n = length(cio.buffer)
    cio.offset += 1
    if cio.offset > n
        resize!(cio.buffer, max(cio.offset, cio.bufsize))
    end
    cio.buffer[cio.offset] = byte
    if cio.offset >= cio.bufsize
        _flush(cio, false)
    end
end

unsafe_write(cio::ChannelIO{R}, ::Ptr{UInt8}, ::UInt) = throw_wrong_mode(cio)
function unsafe_write(cio::ChannelIO{W}, pp::Ptr{UInt8}, nn::UInt)
    isopen(cio) || throw_invalid(cio)
    k = cio.offset
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
        cio.offset += n
    end
    Int(nn)
end

flush(cio::ChannelIO{R}) = throw_wrong_mode(cio)
function flush(cio::ChannelIO{W})
    isopen(cio) || return nothing
    _flush(cio, false)
end

function _flush(cio::ChannelIO{W}, close::Bool)
    cio.offset == 0 && !close && return
    resize!(cio.buffer, cio.offset)
    try
        vput!(cio, cio.buffer)
    catch
        !close && rethrow()
    finally
        cio.position += cio.offset
        newbuffer!(cio)
    end
    nothing
end

function _destroy(cio::ChannelIO{R})
    ch = cio.ch
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
       closefinish(cio)
    catch
        # ignore error during close
    finally
        close(cio.ch)
    end
    nothing
end
closefinish(cio::ChannelIO{R}) = _destroy(cio)
closefinish(cio::ChannelIO{W}) = _flush(cio, true)

peek(cio::ChannelIO{W}) = throw_wrong_mode(cio)
function peek(cio::ChannelIO{R})
    eof(cio) && throw(EOFError())
    cio.buffer[cio.offset + 1]
end

read(cio::ChannelIO{W}, ::Type{UInt8}) = throw_wrong_mode(cio)
function read(cio::ChannelIO{R}, ::Type{UInt8})
    eof(cio) && throw(EOFError())
    c = cio.buffer[cio.offset += 1]
end

bytesavailable(cio::ChannelIO{W}) = throw_wrong_mode(cio)
function bytesavailable(cio::ChannelIO{R})
    n = length(cio.buffer) - cio.offset
    if n > 0
        return Int(n)
    else
        woffset = 0
        cio.offset = length(cio.buffer)
    end
    if isready(cio.ch)
        woffset = takebuffer!(cio)
    end
    Int(woffset)
end

eof(cio::ChannelIO{W}) = throw_wrong_mode(cio)
function eof(cio::ChannelIO{R})
    cio.offset < length(cio.buffer) && return false
    #@infiltrate
    takebuffer!(cio)
    return cio.eofpending && cio.offset >= length(cio.buffer)
end

readbytes!(cio::ChannelIO{W}, ::Vector{UInt8}, ::Any=0; all::Bool=true) = throw_wrong_mode(cio)
function readbytes!(cio::ChannelIO{R}, b::Vector{UInt8}, nb=length(b); all::Bool=true)
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

unsafe_read(cio::ChannelIO{W}, ::Ptr{UInt8}, ::UInt) = throw_wrong_mode(cio)
function unsafe_read(cio::ChannelIO{R}, pp::Ptr{UInt8}, nn::UInt)
    p = pp
    n = Int(nn)
    while n > 0 && !eof(cio)
        k = length(cio.buffer) - cio.offset
        s = pointer(cio.buffer, cio.offset + 1)
        i = min(k, n)
        unsafe_copyto!(p, s, i)
        k -= i
        n -= i
        p += i
        cio.offset += i
    end
    Int(nn - n)
end

function newbuffer!(cio::ChannelIO{W})
    #Vector{UInt8}(undef, cio.bufsize)
    cio.buffer = zeros(UInt8, cio.bufsize)
    cio.offset = 0
end

function takebuffer!(cio::ChannelIO{R})
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
    cio.offset = 0
    cio.position += bufsize
    cio.buffer = buffer
    n = length(buffer)
    cio.eofpending |= n == 0
    n
end

isreadable(::ChannelIO{R}) = true
isreadable(::ChannelIO{W}) = false
iswritable(cio::ChannelIO) = !isreadable(cio)

function position(cio::ChannelIO)
    cio.position + cio.offset
end

function seek(cio::ChannelIO, p::Integer)
    n = length(cio.buffer)
    seek_start = cio.position
    seek_end = cio.position + n
    pp = p - position(cio)
    if seek_start <= p < seek_end
        cio.offset += pp
    else
        boundaries = "$(seek_start) <= $p < $(seek_end)"
        throw(ArgumentError("cannot seek beyond positions ($boundaries)"))
    end
    cio
end

function show(io::IO, cio::ChannelIO{RW}) where RW
    print(io, "ChannelIO{$RW}(")
    showbuf(io, cio)
    print(io, ", position ", position(cio), ", ", channel_state(cio), ")")
end
showbuf(io, cio::ChannelIO{R}) = print(io, channel_length(cio), " → ", buffer_length(cio))
showbuf(io, cio::ChannelIO{W}) = print(io, buffer_length(cio), " → ", channel_length(cio))

function show(io::IO, cio::ChannelPipe)
    print(io, "ChannelPipe(", )
    print(io, buffer_length(cio.in), " → ", channel_length(cio), " → ")
    print(io, buffer_length(cio.out), ", ", channel_state(cio), ")")
end

channel_length(ch::Channel) = (sum(length.(ch.data)), length(ch.data))
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

buffer_length(cio::ChannelIO{R}) = length(cio.buffer) - cio.offset
buffer_length(cio::ChannelIO{W}) = cio.offset

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
