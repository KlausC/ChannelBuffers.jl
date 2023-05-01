abstract type AbstractRemoteClosureList end

"""
    BClosure(f::function, args)

Store function and arguments. The signature of the function must
be like `f(cin::IO, cout::IO, args...)`.
"""
struct BClosure{F,Args<:Tuple}
    f::Symbol
    m::Symbol
    args::Args
    function BClosure(f::Function, args::T) where T
        nf = nameof(f)
        new{nf,T}(nf, nameof(parentmodule(f)), args)
    end
end

function show(io::IO, bc::BClosure)
    print(io, "BClosure(", bc.f, bc.args, ")")
end

# List of BClosure objects and io redirections
struct BClosureList{In,Out}
    list::Vector{Union{BClosure,AbstractCmd,AbstractRemoteClosureList}}
    cin::In
    cout::Out
    @noinline function BClosureList(list, cin::In, cout::Out) where {In,Out}
        new{In,Out}(list, cin, cout)
    end
end
BClosureList(list) = BClosureList(list, DEFAULT_IN, DEFAULT_OUT)

function show(io::IO, bcl::BClosureList)
    arrow = " → "
    print(io, "BClosureList(")
    bcin(bcl) != DEFAULT_IN && print(io, bcin(bcl), arrow)
    print(io, join(bclist(bcl), arrow))
    bcout(bcl) != DEFAULT_OUT && print(io, arrow, bcout(bcl))
    print(io, ")")
end

struct RClosureList{IN,OUT} <: AbstractRemoteClosureList
    id::Int
    bcl::BClosureList{IN,OUT}
end

struct RFileRedirect
    id::Int
    redirect::Base.FileRedirect
end

# used for output redirection
const UIO = Union{IO,AbstractString,Base.FileRedirect,RFileRedirect,Base.RawFD}
const AllIO = Union{UIO,AllChannelIO}
const DEFAULT_IN = devnull
const DEFAULT_OUT = devnull
const BClosureAndList = Union{BClosure,BClosureList}
const BClosureListCmd = Union{BClosureAndList,AbstractCmd}

|(left::BClosureListCmd, right::BClosureAndList) = →(left, right)
|(left::BClosureAndList, right::BClosureListCmd) = →(left, right)
|(left::BClosureAndList, right::BClosureAndList) = →(left, right)

"""
    a → b  (\rightarrow operator)

Convenience function to build a pipeline.
`pipeline(a, b, c)` is essentialy the same as `a → b → c`
"""
→(a, b) = pipeline(a, b)

# pipeline of one Cmd - in analogy to Base
function pipeline(cmd::BClosure; stdin=nothing, stdout=nothing, append=false)
    if stdin === nothing && stdout === nothing
        cmd
    else
        out = append && stdout isa AbstractString ? FileRedirect(stdout, append) : stdout
        BClosureList([cmd], something(stdin, DEFAULT_IN), something(out, DEFAULT_OUT))
    end
end

pipeline(cmd::BClosureList) = cmd
pipeline(left, right) = _pipe(left, right)
pipeline(in::Union{AllChannelIO,BClosure,BClosureList}, cmd::AbstractCmd) = _pipe(in, cmd)
pipeline(cmd::AbstractCmd, out::Union{AllChannelIO,BClosure,BClosureList}) = _pipe(cmd, out)

const BRClosure = Union{BClosure,RClosureList}
const BRClosureList = Union{BRClosure,BClosureList}

_pipe(left::UIO, right::UIO) = pipeline(left, noop(), right)

_pipe(bcl::BRClosureList, out::AllIO) = _pipe(bcl, out, bcliste(bcl))
function _pipe(bcl::BRClosureList, out::AllIO, ble::AbstractCmd)
    BClosureList([bclister(bcl); pipeline(ble, out)], bcin(bcl), bcout(out))
end
_pipe(bcl::BRClosureList, out::AllIO, ::Any) = BClosureList(bclist(bcl), bcin(bcl), out)

_pipe(bcl::BClosureList, out::AllChannelIO) = _pipe(bcl, out, bcliste(bcl))
function _pipe(bcl::BClosureList, out::AllChannelIO, ::AbstractCmd)
    BClosureList([bclist(bcl); noop()], bcin(bcl), out)
end

_pipe(in::AllIO, bcl::BRClosureList) = _pipe(in, bcl, bclistb(bcl))
function _pipe(in::AllIO, bcl::BRClosureList, blb::AbstractCmd)
    BClosureList([pipeline(in, blb); bclistbr(bcl)], bcin(in), bcout(bcl))
end
_pipe(in::AllIO, bcl::BRClosureList, ::Any) = BClosureList(bclist(bcl), in, bcout(bcl))

_pipe(in::AllChannelIO, bcl::BClosureList) = _pipe(in, bcl, bclistb(bcl))
function _pipe(in::AllChannelIO, bcl::BClosureList, ::AbstractCmd)
    BClosureList([noop(); bclist(bcl)], in, bcout(bcl))
end

function _pipe(bc::BRClosureList, bcl::BRClosureList)
    BClosureList([bclist(bc); bclist(bcl)], bcin(bc), bcout(bcl))
end

_pipe(cmd::AbstractCmd, out::AllChannelIO) = pipeline(cmd, noop(), out)
_pipe(in::AllChannelIO, cmd::AbstractCmd) = pipeline(in, noop(), cmd)

_pipe(cmd::AbstractCmd, bc::BRClosure) = _pipe(cmd, bc, nothing)
_pipe(bc::BRClosure, cmd::AbstractCmd) = _pipe(bc, cmd, nothing)

_pipe(cmd::AbstractCmd, bcl::BClosureList) = _pipe(cmd, bcl, bclistb(bcl))
function _pipe(cmd::AbstractCmd, bcl::BClosureList, rlb::AbstractCmd)
    BClosureList([pipeline(cmd, rlb); bclistbr(bcl)], bcin(cmd), bcout(bcl))
end
function _pipe(cmd::AbstractCmd, bcl::BRClosureList, ::Any)
    BClosureList([cmd; bclist(bcl)], bcin(cmd), bcout(bcl))
end

_pipe(bcl::BClosureList, cmd::AbstractCmd) = _pipe(bcl, cmd, bcliste(bcl))
function _pipe(bcl::BClosureList, cmd::AbstractCmd, lle::AbstractCmd)
    BClosureList([bclister(bcl); pipeline(lle, cmd)], bcin(bcl), bcout(cmd))
end
function _pipe(bcl::BRClosureList, cmd::AbstractCmd, ::Any)
    BClosureList([bclist(bcl); cmd], bcin(bcl), bcout(cmd))
end

function _pipe(bcll::BClosureList, bclr::BClosureList)
    lle = bcliste(bcll)
    rlb = bclistb(bclr)
    cmd = pipeline(lle, rlb)
    BClosureList([bclister(bcll); bclist(cmd); bclistbr(bclr)], bcll.cin, bclr.cout)
end

bclist(bcl::BClosureList) = bcl.list
bclist(a::BRClosureList) = [a]

bclistb(bcl::BRClosureList) = bclist(bcl)[begin]
bclistbr(bcl::BRClosureList) = bclist(bcl)[begin+1:end]
bcliste(bcl::BRClosureList) = bclist(bcl)[end]
bclister(bcl::BRClosureList) = bclist(bcl)[begin:end-1]

bcin(bcl::BClosureList) = bcl.cin
bcin(::Any) = DEFAULT_IN
bcout(bcl::BClosureList) = bcl.cout
bcout(::Any) = DEFAULT_OUT
