"""
    BClosure(f::function, args)

Store function and arguments. The signature of the function must
be like `f(cin::IO, cout::IO, args...)`.
"""
struct BClosure{F<:Function,Args<:Tuple}
    f::F
    args::Args
end

import Base: |, AbstractCmd, pipeline

# used for output redirection
const UIO = Union{IO,AbstractString}
const AllIO = Union{UIO,AllChannelIO}
const DEFAULT_IN = devnull
const DEFAULT_OUT = devnull

const ClosureCmd = Union{BClosure,AbstractCmd}

# List of BClosure objects an io redirections
struct BClosureList{In,Out}
    list::Vector{ClosureCmd}
    cin::In
    cout::Out
    @noinline function BClosureList(list, cin::In, cout::Out) where {In,Out}
        #println("BClosureList($list)")
        new{In,Out}(list, cin, cout)
    end
end
BClosureList(list) = BClosureList(list, DEFAULT_IN, DEFAULT_OUT)

struct BTask{X,T}
    task::T
    BTask{X}(t::T) where {X,T} = new{X,T}(t) 
end
Base.show(io::IO, m::MIME"text/plain", bt::BTask) = show(io, m, bt.task)
Base.fetch(bt::BTask) = fetch(bt.task)
Base.wait(bt::BTask) = wait(bt.task)
Base.istaskstarted(bt::BTask) = istaskstarted(bt.task)
Base.istaskdone(bt::BTask) = istaskdone(bt.task)
Base.istaskfailed(bt::BTask) = istaskfailed(bt.task)

# List of tasks - output of `schedule`
struct BTaskList{V<:Vector{<:BTask}}
    list::V
end

|(left::Union{BClosure,BClosureList}, right::Union{BClosure,BClosureList}) = →(left, right)
"""
    a → b  (\rightarrow operator)

Convenience function to build a pipeline.
`pipeline(a, b, c)` is essentialy the same as `a → b → c`
"""
→(a, b) = pipeline(a, b)
→(ci::UIO, co::UIO) = pipeline(ci, NOOP, co)

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

# insert a NOOP task to redirect ChannelIO to/from AbstractCmd
# combine AbstractCmd with other IO
listnoop(io::UIO, cmd::AbstractCmd) = [pipeline(cmd, stdin=io)]
listnoop(io::AllChannelIO, cmd::AbstractCmd) = [NOOP, cmd]
listnoop(io::AllIO, cmd::ClosureCmd) = [cmd]
listnoop(cmd::AbstractCmd, io::UIO) = [pipeline(cmd, stdout=io)]
listnoop(cmd::AbstractCmd, io::AllChannelIO) = [cmd, NOOP]
listnoop(cmd::ClosureCmd, io::AllIO) = [cmd]

listnoop(io::AllIO, v::Vector) = listnoop(io, first(v), v)
listnoop(v::Vector, io::AllIO) = listnoop(v, last(v), io)

listnoop(v::Vector, vcmd::ClosureCmd, io::AllIO) = vcat(v[1:end-1], listnoop(v[end], io))
listnoop(io::AllIO, vcmd::ClosureCmd, v::Vector) = vcat(listnoop(io, v[1]), v[2:end])

# pipeline of one Cmd - in analogy to Base
function pipeline(cmd::BClosure; stdin=nothing, stdout=nothing)
    if stdin === nothing && stdout === nothing
        cmd
    else
        BClosureList([cmd], something(stdin,DEFAULT_IN), something(stdout, DEFAULT_OUT))
    end
end

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

"""
    wait(tl::BTaskList)

Wait for the last task in the list to finish.
"""
function Base.wait(tv::BTaskList, ix::Integer=0)
    n = length(tv)
    i = ix == 0 ? n : Int(ix)
    0 < n || return nothing
    0 < i <= n || throw(BoundsError(tv.list, i))
    @inbounds wait(tv.list[i])
end

"""
    fetch(tl::BTaskList)

Wait for last Task in to finish, then return its result value.
If the task fails with an exception, a `TaskFailedException` (which wraps the failed task) is thrown.
"""
function Base.fetch(tv::BTaskList, ix::Integer=0)
    n = length(tv)
    i = ix == 0 ? n : Int(ix)
    0 < n || throw(ArgumentError("cannot fetch from empty task list"))
    0 < i <= n || throw(BoundsError(tv.list, i))
    @inbounds fetch(tv.list[i])
end

Base.length(tv::BTaskList) = length(tv.list)
Base.getindex(tv::BTaskList, i) = getindex(tv.list, i)
function Base.show(io::IO, m::MIME"text/plain", tv::BTaskList)
    for t in tv
        show(io, m, t)
        println(io)
    end
end
Base.iterate(tl::BTaskList, s...) = iterate(tl.list, s...)

"""
    task_code, task_cin, task_cout, task_function, task_args

Access the argumentless function provided to the task
"""
task_code(t::BTask{:Task}) = t.task.code
task_code(t::BTask{:Threat}) = t.task.code.task_function
task_cin(t::BTask) = task_code(t).cin
task_cout(t::BTask) = task_code(t).cout
task_function(t::BTask) = task_code(t).btd.f
task_args(t::BTask) = task_code(t).btd.args

"""
    run(BClosure; stdin=devnull, stdout=devnull)

Start parallel task redirecting stdin and stdout
"""
function Base.run(bt::BClosure; stdin=DEFAULT_IN, stdout=DEFAULT_OUT)
    run(BClosureList([bt], stdin, stdout))
end

"""
    run(::BClosureList)

Start all parallel tasks defined in list, io redirection defaults are defined in the list
"""
function Base.run(btdl::BClosureList)
    T = use_tasks() ? :Task : :Threat
    list = btdl.list
    n = length(list)
    tl = Vector{Union{Task,Base.AbstractPipe}}(undef, n)
    n > 0 || return BTaskList(BTask{T}.(tl))

    cout = btdl.cout
    i = n
    while i > 1
        s = list[i]
        if s isa AbstractCmd
            cout = open(s, write=true, read= i == n)
            tl[i] = cout
            i -= 1
        else
            if list[i-1] isa AbstractCmd
                cin = open(list[i-1], read=true, write = i > 2)
                tl[i-1] = cin
                di = 2
            else
                cin = ChannelPipe()
                di = 1
            end
            tl[i] = _schedule(s, cin, cout)
            cout = cin
            i -= di
        end
    end
    if i >= 1
        tl[1] = _schedule(btdl.list[1], btdl.cin, cout)
    end
    BTaskList(BTask{T}.(tl))
end

"""
    
(f::Function, args...)

Generate a `BClosure` object, which can be used to be started in parallel.
The function `f` must have the signature `f(cin::IO, cout::IO [, args...])`.
It may be wrapped in an argumentless closure to be used in a `Task` definition.
"""
function closure(f::Function, args...)
    BClosure(f, args)
end

function use_tasks()
    Threads.nthreads() <= 1 || Threads.threadid() != 1
end

# schedule single task
function _schedule(btd::BClosure, cin, cout)
    function task_function()
        ci, hr = vopen(cin)
        co, hw = vopen(cout, true)
        try
            btd.f(ci, co, btd.args...)
        finally
            vclose(hr, ci)
            vclose(hw, co, true)
        end
    end
    if Threads.nthreads() <= 1 || Threads.threadid() != 1
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end

vopen(file::AbstractString, write=false) = (open(file, write=write), true)
vopen(fr::Base.FileRedirect, write=false) = (open(fr.filename, write=write, append=fr.append), true)
vopen(cio, write=false) = (cio, false)

vclose(here::Bool, cio, write=false) = here ? close(cio) : vclose(cio, write)

vclose(cio::Base.TTY, write) = nothing # must not be changed to avoid REPL kill
vclose(cio::IOContext, write) = vclose(cio.io, write)
function vclose(cio::Base.AbstractPipe, write)
    w = Base.pipe_writer(cio)
    write && isopen(w) ? close(w) : nothing
end
vclose(cio::ChannelIO, write) = write && isopen(cio) ? close(cio) : nothing
vclose(cio::ChannelPipe, write) = vclose(Base.pipe_writer(cio), write)
vclose(cio::Channel, write) = isopen(cio) ? close(cio) : nothing
vclose(cio, write) = nothing


function _noop(cin::IO, cout::IO)
    b = Vector{UInt8}(undef, DEFAULT_BUFFER_SIZE)
    while !noop_eof(cin)
        x = try
            x = noop_read(cin, b)
            noop_write(cout, x)
        catch ex
            ex isa InvalidStateException || rethrow(ex)
        end
    end
end
function noop()
    closure(_noop)
end

noop_eof(ci::IO) = eof(ci)
noop_read(ci::IO, b) = b[1:readbytes!(ci, b)]
noop_write(co::IO, x) = write(co, x)

noop_eof(ch::Channel) = isempty(ch) && !isopen(ch)
noop_read(ch::Channel) = take!(ch)
noop_write(ch::Channel, x) = put!(ch, x)

noop_eof(ci::ChannelIO) = noop_eof(ci.ch)
noop_read(ci::ChannelIO, b) = begin x = noop_read(ci.ch); ci.position += sizeof(x); x end
function noop_write(co::ChannelIO, x)
    co.position += sizeof(x)
    noop_write(co.ch, x)
end

noop_eof(ci::ChannelPipe) = noop_eof(Base.pipe_reader(ci))
noop_read(ci::ChannelPipe, b) = noop_read(Base.pipe_reader(ci), b)
noop_write(co::ChannelPipe, b) = noop_write(Base.pipe_writer(co), b)

"""
    const NOOP

A BClosure which copies input to output unmodified.
"""
const NOOP = noop()
