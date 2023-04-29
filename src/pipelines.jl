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

const ClosureCmd = Union{BClosure,AbstractCmd}
# List of BClosure objects and io redirections
struct BClosureList{In,Out}
    list::Vector{ClosureCmd}
    cin::In
    cout::Out
    @noinline function BClosureList(list, cin::In, cout::Out) where {In,Out}
        new{In,Out}(list, cin, cout)
    end
end
BClosureList(list) = BClosureList(list, DEFAULT_IN, DEFAULT_OUT)
# used for output redirection
const UIO = Union{IO,AbstractString}
const AllIO = Union{UIO,AllChannelIO}
const DEFAULT_IN = devnull
const DEFAULT_OUT = devnull

const BClosureAndList = Union{BClosure,BClosureList}
const BClosureListCmd = Union{BClosureAndList,AbstractCmd}

|(left::BClosureListCmd, right::BClosureListCmd) = →(left, right)

"""
    a → b  (\rightarrow operator)

Convenience function to build a pipeline.
`pipeline(a, b, c)` is essentialy the same as `a → b → c`
"""
→(a, b) = pipeline(a, b)
→(ci::UIO, co::UIO) = pipeline(ci, noop(), co)

# combine 2 AbstractCmd into a pipeline
listcombine(cmd::ClosureCmd, v::Vector) = isempty(v) ? [cmd] : listcombine(cmd, first(v), v)
listcombine(v::Vector, cmd::ClosureCmd) = isempty(v) ? [cmd] : listcombine(v, last(v), cmd)
listcombine(left::ClosureCmd, right::ClosureCmd) = vcat(left, right)
listcombine(left::AbstractCmd, right::AbstractCmd) = [pipeline(left, right)]
listcombine(list::Vector, ::ClosureCmd, right::ClosureCmd) = vcat(list, right)
listcombine(list::Vector, ::AbstractCmd, right::AbstractCmd) = vcat(list[1:end-1], listcombine(last(list), right))
listcombine(left::ClosureCmd, ::ClosureCmd, list::Vector) = vcat(left, list)
listcombine(left::AbstractCmd, ::AbstractCmd, list::Vector) = vcat(listcombine(left, first(list)), list[2:end])

function listcombine(left::Vector, right::Vector)
    isempty(right) && return left
    isempty(left) && return right
    listcombine(listcombine(left, first(right)), right[2:end])
end

# insert a noop() task to redirect ChannelIO to/from AbstractCmd
# combine AbstractCmd with other IO
listnoop(io::UIO, cmd::AbstractCmd) = [pipeline(cmd, stdin=io)]
listnoop(io::AllChannelIO, cmd::AbstractCmd) = [noop(), cmd]
listnoop(io::AllIO, cmd::ClosureCmd) = [cmd]
listnoop(cmd::AbstractCmd, io::UIO) = [pipeline(cmd, stdout=io)]
listnoop(cmd::AbstractCmd, io::AllChannelIO) = [cmd, noop()]
listnoop(cmd::ClosureCmd, io::AllIO) = [cmd]

listnoop(io::AllIO, v::Vector) = listnoop(io, first(v), v)
listnoop(v::Vector, io::AllIO) = listnoop(v, last(v), io)

listnoop(v::Vector, vcmd::ClosureCmd, io::AllIO) = vcat(v[1:end-1], listnoop(v[end], io))
listnoop(io::AllIO, vcmd::ClosureCmd, v::Vector) = vcat(listnoop(io, v[1]), v[2:end])

# pipeline of one Cmd - in analogy to Base
function pipeline(cmd::BClosure; stdin=nothing, stdout=nothing, append=false)
    if stdin === nothing && stdout === nothing
        cmd
    else
        out = append && stdout isa AbstractString ? FileRedirect(stdout, append) : stdout
        BClosureList([cmd], something(stdin,DEFAULT_IN), something(out, DEFAULT_OUT))
    end
end

pipeline(cmd::BClosureList) = cmd

# combine two commands - except case AbstractCmd/AbstractCmd, which is in Base
pipeline(left::AbstractCmd, right::BClosure) = BClosureList(listcombine(left, right), DEFAULT_IN, DEFAULT_OUT)
pipeline(left::BClosure, right::ClosureCmd) = BClosureList(listcombine(left, right), DEFAULT_IN, DEFAULT_OUT)

# combine Cmd with list
function pipeline(left::BClosureList, right::ClosureCmd)
    BClosureList(listcombine(left.list, right), left.cin, DEFAULT_OUT)
end
pipeline(left::AbstractCmd, right::BClosureList) = BClosureList(listcombine(left, right.list), DEFAULT_IN, right.cout)
pipeline(left::BClosure, right::BClosureList) = BClosureList(listcombine(left, right.list), DEFAULT_IN, right.cout)

# combine Cmd with IO
pipeline(left::AllIO, right::BClosure) = BClosureList(listnoop(left, right), left, DEFAULT_OUT)
pipeline(left::BClosure, right::AllIO) = BClosureList(listnoop(left, right), DEFAULT_IN, right)
pipeline(left::AllChannelIO, right::AbstractCmd) = BClosureList(listnoop(left, right), left, DEFAULT_OUT)
pipeline(left::AbstractCmd, right::AllChannelIO) = BClosureList(listnoop(left, right), DEFAULT_IN, right)
# AbstractCmd with UIO is in Base

# combine two lists
function pipeline(left::BClosureList, right::BClosureList)
    BClosureList(listcombine(left.list, right.list), left.cin, right.cout)
end

# combine list with IO
pipeline(left::AllIO, right::BClosureList) = BClosureList(listnoop(left, right.list), left, right.cout)
pipeline(left::BClosureList, right::AllIO) = BClosureList(listnoop(left.list, right), left.cin, right)
