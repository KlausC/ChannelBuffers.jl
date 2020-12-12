
abstract type AbstractChannelIO <: IO end

"""
    ChannelIO

Is read and written like an `IOStream`. If buffers are empty/full transport them using
channels allowing parallel task pipelining.
"""
mutable struct ChannelIO{T<:AbstractVector{UInt8}} <: AbstractChannelIO
    ch::Channel{T}
    rw::Symbol # :R readonly :W writeonly
    buffer::T
    bufsize::Int
    woffset::Int
    roffset::Int
    eofpending::Bool
    position::Int
    mark::Int
    function ChannelIO(ch::Channel, rw::Symbol, buffer::V, bufsize::Integer) where V
        bufsize > 0 || throw(ArgumentError("minimal buffer size is $bufsize"))
        rw == :R || rw == :W || throw(ArgumentError("read/write must be :R or :W"))
        new{V}(ch, rw, buffer, bufsize, 0, 0, false, 0, -1)
    end
end
 
const DEFAULT_BUFFER_SIZE = 8192 
const DEFAULT_CHANNEL_LENGTH = 1

struct ChannelPipe{T} <: Base.AbstractPipe
    in::ChannelIO{T}
    out::ChannelIO{T}
    function ChannelPipe(bufsize::Integer=DEFAULT_BUFFER_SIZE)
        out = ChannelIO(bufsize)
        in = reverseof(out)
        new{typeof(in.buffer)}(in, out)
    end
end

const AllChannelIO = Union{ChannelIO,ChannelPipe}

function ChannelIO(ch::Channel, rw::Symbol=:R, bufsize::Integer=DEFAULT_BUFFER_SIZE)
    ChannelIO(ch, rw, zeros(UInt8, 0), bufsize)
end

function ChannelIO(rw::Symbol, bufsize::Integer=DEFAULT_BUFFER_SIZE)
    T = Vector{UInt8}
    ch = Channel{T}(DEFAULT_CHANNEL_LENGTH)
    ChannelIO(ch, rw, bufsize)
end

function ChannelIO(bufsize::Integer=DEFAULT_BUFFER_SIZE)
    ChannelIO(:R, bufsize)
end

# create ChannelIO with smae channel and reverse read/write indicator
function reverseof(cio::ChannelIO)
    rev = cio.rw == :R ? :W : :R
    ChannelIO(cio.ch, rev, cio.bufsize)
end

function Base.isopen(cio::ChannelIO)
    isopen(cio.ch) || isready(cio.ch)
end

function throw_inv(cio::ChannelIO)
    throw(InvalidStateException("channel is $(cio.rw)-only", cio.rw))
end

function Base.write(cio::ChannelIO, byte::UInt8)
    cio.rw == :W || throw_inv(cio)
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

function Base.unsafe_write(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    cio.rw == :W || throw_inv(cio)
    k = cio.woffset
    if length(cio.buffer) < cio.bufsize
        resize!(cio.buffer, cio.bufsize)
    end
    d = pointer(cio.buffer, k+1)
    s = pp
    n = Int(nn)
    while n + k >= cio.bufsize
        i = cio.bufsize - k
        unsafe_copyto!(d, s, i)
        put!(cio.ch, cio.buffer)
        cio.position += cio.woffset
        cio.buffer = getbuffer(cio)
        d = pointer(cio.buffer, 1)
        cio.woffset = 0
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

function Base.flush(cio::ChannelIO)
    cio.rw == :W || return nothing
    _flush(cio, false)
end

function _flush(cio::ChannelIO, eofsend::Bool)
    cio.woffset == 0 && !eofsend && return
    resize!(cio.buffer, cio.woffset)
    put!(cio.ch, cio.buffer)
    cio.position += cio.woffset
    cio.buffer = getbuffer(cio)
    cio.woffset = 0
    cio.roffset = 0
    nothing
end

function Base.close(cio::ChannelIO)
    isopen(cio.ch) || return
    _flush(cio, true)
    cio.bufsize = 0
    close(cio.ch)
    nothing
end

function Base.peek(cio::ChannelIO)
    cio.rw  == :R || throw_inv(cio)
    eof(cio) && throw(EOFError())
    cio.buffer[cio.roffset + 1]
end
function Base.read(cio::ChannelIO, ::Type{UInt8})
    cio.rw == :R || throw_inv(cio)
    eof(cio) && throw(EOFError())
    c = cio.buffer[cio.roffset += 1]
end

function Base.bytesavailable(cio::ChannelIO)
    cio.rw  == :R || throw_inv(cio)
    n = cio.woffset - cio.roffset
    if n > 0
        return Int(n)
    else
        cio.eofpending && return 0
        cio.woffset = 0
        cio.roffset = 0
    end
    if isready(cio.ch)
        takebuffer!(cio)
    end
    Int(cio.woffset)
end

function Base.eof(cio::ChannelIO)
    cio.rw  == :R || throw_inv(cio)
    cio.roffset < cio.woffset && return false
    takebuffer!(cio)
    return cio.eofpending && cio.roffset >= cio.woffset
end

function Base.readbytes!(cio::ChannelIO, b::Vector{UInt8}, nb=length(b); all::Bool=true)
    s = bytesavailable(cio)
    n = min(s, nb)
    if n > length(b)
        resize!(b, n)
    end
    r = Base.unsafe_read(cio, pointer(b, 1), n)
    if !all || r >= nb
        return Int(r)
    end
    while r < nb && !eof(cio)
        n = min(nb, r + 1024)
        if n > length(b)
            resize!(b, n)
        end
        d = pointer(b, r + 1)
        k = Base.unsafe_read(cio, d, n - r)
        r += k
    end
    return Int(r)
end

function Base.unsafe_read(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    cio.rw  == :R || throw_inv(cio)
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

function getbuffer(cio::ChannelIO)
    #Vector{UInt8}(undef, cio.bufsize)
    zeros(UInt8, cio.bufsize)
end

function takebuffer!(cio::ChannelIO)
    if !isopen(cio.ch) && !isready(cio.ch)
        cio.eofpending = true
        cio.woffset = 0
    else
        try
            cio.buffer = take!(cio.ch)
            cio.woffset = length(cio.buffer)
            cio.position += cio.woffset
            cio.eofpending |= cio.woffset == 0
        catch
            cio.woffset = 0
            cio.eofpending = true
        end
    end
    cio.roffset = 0
    cio.woffset
end

Base.isreadable(cio::ChannelIO) = cio.rw == :R
Base.iswritable(cio::ChannelIO) = cio.rw == :W

function Base.position(cio::ChannelIO)
    cio.position + (cio.rw == :W ? cio.woffset : cio.roffset - cio.woffset)
end

Base.seek(cio::ChannelIO, p) = isreadable(cio) ? seek_r(cio, p) : seek_w(cio, p)

function seek_r(cio::ChannelIO, p::Integer)
    if cio.position - cio.woffset <= p <= cio.position
        cio.roffset = p - cio.position + cio.woffset
    else
        boundaries = "$(cio.position - cio.woffset) <= $p <= $(cio.position)"
        throw(ArgumentError("cannot seek beyond positions ($boundaries)"))
    end
    cio
end
function seek_w(cio::ChannelIO, p::Integer)
    if cio.position + cio.roffset <= p <= cio.position + cio.woffset
        cio.woffset = p - cio.position
    else
        boundaries = "$(cio.position + cio.roffset) <= $p <= $(cio.woffset+cio.position)"
        throw(ArgumentError("cannot seek beyond positions ($boundaries)"))
    end
    cio
end

function Base.show(io::IO, cio::ChannelIO)
    print(io, "ChannelIO(:", cio.rw, ", ", cio.woffset - cio.roffset, " bytes available)")
end

function Base.show(io::IO, cp::ChannelPipe)
    write(io, "ChannelPipe(")
    show(io, cp.in)
    write(io, " => ", string(buffered_length(cp)), " bytes buffered => ")
    show(io, cp.out)
    write(io, ")")
end

buffered_length(ch::Channel) = sum(length.(ch.data))
buffered_length(cio::ChannelIO) = buffered_length(cio.ch)
buffered_length(cp::ChannelPipe) = buffered_length(cp.in)

Base.write(io::ChannelPipe, x::UInt8) = write(io.in, x)
Base.unsafe_write(io::ChannelPipe, p::Ptr{UInt8}, n::UInt) = unsafe_write(io.in, p, n)
Base.flush(io::ChannelPipe) = flush(io.in)
Base.unsafe_read(io::ChannelPipe, p::Ptr{UInt8}, n::UInt) = unsafe_read(io.out, p, n)
Base.eof(io::ChannelPipe) = eof(io.out)
Base.bytesavailable(io::ChannelPipe) = bytesavailable(io.out)
Base.peek(io::ChannelPipe) = peek(io.out)
Base.readbytes!(io::ChannelPipe, p::AbstractVector{UInt8}, n=length(p)) = readbytes!(io.out, p, n)
Base.read(io::ChannelPipe, T::Type{UInt8}) = read(io.out, T)
Base.pipe_reader(io::ChannelPipe) = io.out
Base.pipe_writer(io::ChannelPipe) = io.in
Base.close(io::ChannelPipe) = begin close(io.in); close(io.out) end
