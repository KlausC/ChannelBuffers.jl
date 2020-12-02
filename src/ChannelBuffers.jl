module ChannelBuffers

export ChannelIO

"""
    ChannelIO

Is read and written like an `IOStream`. If buffers are empty/full transport them using
channels allowing parallel task pipelining.
"""
mutable struct ChannelIO{T<:AbstractVector{UInt8}} <: IO
    ch::Channel{T}
    buffer::T
    bufsiz::UInt
    offset::UInt
    read::UInt
    eofpending::Bool
    function ChannelIO(ch::Channel, buffer::V, bufsiz::Integer) where V
        bufsiz > 0 || throw(ArgumentError("minimal buffer size is $bufsiz"))
        new{V}(ch, buffer, bufsiz, 0, 0, false)
    end
end

const DEFAULT_BUFFER_SIZE = 1024
const DEFAULT_CHANNEL_LENGTH = 1

function ChannelIO(ch::Channel, bufsiz::Integer=DEFAULT_BUFFER_SIZE)
    ChannelIO(ch, zeros(UInt8, 0), bufsiz)
end

function ChannelIO(bufsiz::Integer=DEFAULT_BUFFER_SIZE)
    T = Vector{UInt8}
    ch = Channel{T}(DEFAULT_CHANNEL_LENGTH)
    ChannelIO(ch, bufsiz)
end

function Base.unsafe_write(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    k = cio.offset
    if length(cio.buffer) < cio.bufsiz
        resize!(cio.buffer, cio.bufsiz)
    end
    d = pointer(cio.buffer, k+1)
    s = pp
    n = nn
    while n + k >= cio.bufsiz
        i = cio.bufsiz - k
        unsafe_copyto!(d, s, i)
        put!(cio.ch, cio.buffer)
        cio.buffer = getbuffer(cio)
        d = pointer(cio.buffer, 1)
        cio.offset = 0
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

function Base.flush(cio::ChannelIO)
    _flush(cio, false)
end

function _flush(cio::ChannelIO, eofsend::Bool)
    cio.offset == 0 && !eofsend && return
    resize!(cio.buffer, cio.offset)
    put!(cio.ch, cio.buffer)
    cio.buffer = getbuffer(cio)
    cio.offset = 0
    cio.read = 0
    nothing
end

function Base.close(cio::ChannelIO)
    _flush(cio, true)
    cio.bufsiz = 0
    close(cio.ch)
    nothing
end

function Base.unsafe_read(cio::ChannelIO, pp::Ptr{UInt8}, nn::UInt)
    p = pp
    n = nn
    while n > 0 && !eof(cio)
        k = cio.offset - cio.read
        s = pointer(cio.buffer, cio.read + 1)
        i = min(k, n)
        unsafe_copyto!(p, s, i)
        k -= i
        n -= i
        p += i
        cio.read += i
    end
    Int(nn - n)
end

function Base.peek(cio::ChannelIO)
    eof(cio) && throw(EOFError())
    cio.buffer[cio.read + 1]
end
function Base.read(cio::ChannelIO, ::Type{UInt8})
    eof(cio) && throw(EOFError())
    c = cio.buffer[cio.read += 1]
end

function Base.bytesavailable(cio::ChannelIO)
    n = cio.offset - cio.read
    if n > 0
        return Int(n)
    else
        cio.eofpending && return 0
        cio.offset = 0
        cio.read = 0
    end
    if isready(cio.ch)
        takebuffer!(cio)
    end
    Int(cio.offset)
end

function Base.eof(cio::ChannelIO)
    cio.read < cio.offset && return false
    takebuffer!(cio)
    return cio.eofpending && cio.read >= cio.offset
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

function getbuffer(cio::ChannelIO)
    #Vector{UInt8}(undef, cio.bufsiz)
    zeros(UInt8, cio.bufsiz)
end

function takebuffer!(cio::ChannelIO)
    if !isopen(cio.ch) && !isready(cio.ch)
        cio.eofpending = true
        cio.offset = 0
    else
        cio.buffer = take!(cio.ch)
        cio.offset = length(cio.buffer)
        cio.eofpending |= cio.offset == 0
    end
    cio.read = 0
    cio.offset
end

include("tasks.jl")

end # module
