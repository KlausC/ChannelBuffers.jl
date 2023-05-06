
const DEFAULT_IN = devnull
const DEFAULT_OUT = devnull
abstract type AbstractRClosure end

"""
    BClosure(f::function, args)

Store identifier of function and all arguments. The signature of the function must
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

"""
    BClosureList{In,Out}

List of `BClosure`, `AbstractCmd`, and `RClosure` objects
together with input- and output redirections.
"""
struct BClosureList{In,Out}
    list::Vector{Union{BClosure,AbstractCmd,AbstractRClosure}}
    cin::In
    cout::Out
    @noinline function BClosureList(list, cin::In, cout::Out) where {In,Out}
        new{In,Out}(list, cin, cout)
    end
end
BClosureList(list) = BClosureList(list, DEFAULT_IN, DEFAULT_OUT)

"""
    RClosure{In,Out}

A "remote" `BClosureList` and a process-identifier used for `Distributed`
processing. The io is relative to the remote location.
Remote sites are external processes, running a julia-image, on the same
of on different machines.
"""
struct RClosure{In,Out} <: AbstractRClosure
    id::Int
    bcl::BClosureList{In,Out}
end

"""
    RFileRedirect

A remote file redirection, which adds the process-id of `Distributed`
to a file redirection (file-name and append flag).
"""
struct RFileRedirect
    id::Int
    redirect::Base.FileRedirect
end

# used for output redirection
const UIO = Union{Redirectable,AbstractString,RFileRedirect}
const AllIO = Union{UIO,AllChannelIO}

const BRClosure = Union{BClosure,RClosure}
const BRClosureList = Union{BRClosure,BClosureList}
const BRClosuresCmd = Union{BRClosureList,AbstractCmd}

# customized `show` methods
function show(io::IO, bc::BClosure)
    print(io, "BClosure(", bc.f, bc.args, ")")
end

function show(io::IO, bcl::BClosureList)
    arrow = " → "
    print(io, "BClosureList(")
    bcin(bcl) != DEFAULT_IN && print(io, bcin(bcl), arrow)
    print(io, join(bclist(bcl), arrow))
    bcout(bcl) != DEFAULT_OUT && print(io, arrow, bcout(bcl))
    print(io, ")")
end

# support remote operations
function at(id::Integer, file::AbstractString, append::Bool=false)
    at(id, Base.FileRedirect(file, append))
end
file_at(id::Integer, redir::Base.FileRedirect) = RFileRedirect(id, redir)

at(id::Integer, bcl::BClosureList) = RClosure(id, bcl)
at(id::Integer, bc::BClosure) = RClosure(id, BClosureList([bc]))
at(id::Integer, rcl::RClosure) = RClosure(id, rcl.bcl)

# Convenience operator - avoid type piracy for Cmd-Cmd case
|(left::BRClosuresCmd, right::BRClosureList) = →(left, right)
|(left::BRClosureList, right::BRClosuresCmd) = →(left, right)
|(left::BRClosureList, right::BRClosureList) = →(left, right)

"""
    a → b  (\rightarrow operator)

Convenience function to build a pipeline.
`pipeline(a, b, c)` is essentialy the same as `a → b → c`.

(This operator is right-associative and less binding than `|`
Both properties are irrelevant for the usage in pipelines)
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
pipeline(in::Union{AllChannelIO,BRClosureList}, cmd::AbstractCmd) = _pipe(in, cmd)
pipeline(cmd::AbstractCmd, out::Union{AllChannelIO,BRClosureList}) = _pipe(cmd, out)

# Joining two objects or lists of objects
# 1. BClosure, RClosure, BClosureList
function _pipe(bc::BRClosureList, bcl::BRClosureList)
    BClosureList([bclist(bc); bclist(bcl)], bcin(bc), bcout(bcl))
end

function _pipe(bcll::BClosureList, bclr::BClosureList)
    lle = bcliste(bcll)
    rlb = bclistb(bclr)
    cmd = pipeline(lle, rlb)
    BClosureList([bclister(bcll); bclist(cmd); bclistbr(bclr)], bcll.cin, bclr.cout)
end

# 2. IO and IO
_pipe(left::UIO, right::UIO) = pipeline(pipeline(left, noop()), right)

# 3. IO and (BClosure, RColsureList, BClosureList)
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

# 4. Cmd and IO
_pipe(cmd::AbstractCmd, out::AllChannelIO) = pipeline(cmd, noop(), out)
_pipe(in::AllChannelIO, cmd::AbstractCmd) = pipeline(in, noop(), cmd)

# 5. Cmd and (BClosure, RClosure, BClosureList)
_pipe(cmd::AbstractCmd, bc::BRClosure) = _pipe(cmd, bc, bc)
_pipe(bc::BRClosure, cmd::AbstractCmd) = _pipe(bc, cmd, bc)

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

# Accessors for BClosure, RClosure, BClosure
bclist(bcl::BClosureList) = bcl.list
bclist(a::Any) = [a]

bclistb(bcl::BRClosureList) = bclist(bcl)[begin]
bclistbr(bcl::BRClosureList) = bclist(bcl)[begin+1:end]
bcliste(bcl::BRClosureList) = bclist(bcl)[end]
bclister(bcl::BRClosureList) = bclist(bcl)[begin:end-1]

bcin(bcl::BClosureList) = bcl.cin
bcin(::Any) = DEFAULT_IN
bcout(bcl::BClosureList) = bcl.cout
bcout(::Any) = DEFAULT_OUT
