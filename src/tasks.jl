
struct BTask{X,T}
    task::T
    BTask{X}(t::T) where {X,T} = new{X,T}(t)
end

function show(io::IO, m::MIME"text/plain", bt::BTask)
    nprocs() > 1 && print(io, "@", myid(), " ")
    show(io, m, bt.task)
end
function show(io::IO, m::MIME"text/plain", bt::BTask{T,Task} where T)
    t = bt.task
    nprocs() > 1 && print(io, "@", myid(), " ")
    show(io, m, t)
    bc = task_code(bt).bc
    print(io, " ", bc.f, bc.args)
end
fetch(bt::BTask) = fetch(bt.task)
wait(bt::BTask) = wait(bt.task)
istaskstarted(bt::BTask)  = istaskstarted(bt.task)
istaskdone(bt::BTask) = istaskdone(bt.task)
istaskfailed(bt::BTask) = istaskfailed(bt.task)

function fetch(bt::BTask{T,<:Process}) where T
    wait(bt)
    bt.task.exitcode
end
istaskstarted(bt::BTask{T,<:Process}) where T = true
istaskdone(bt::BTask{T,<:Process}) where T = process_exited(bt.task)
function istaskfailed(bt::BTask{T,<:Process}) where T
    process_exited(bt.task) && bt.task.exitcode != 0
end

# List of tasks and io redirections - output of `run` and `open`
"""
    TaskChain

An `AbstractPipe` wich contains a list of running tasks and can be used for
reading and writing. Created by a call to `[open|run](::BClosureList)`.
A call with `fetch` waits for and returns the result of the last task of chain.

An analogue of `ProcessChain`.
"""
struct TaskChain{T<:AbstractVector{<:BTask},IN<:IO,OUT<:IO} <: AbstractPipe
    processes::T
    in::IN
    out::OUT
end

"""
    wait(tl::TaskChain)

Wait for the last task in the list to finish.
"""
function wait(tv::TaskChain, ix::Integer=0)
    n = length(tv)
    i = ix == 0 ? n : Int(ix)
    0 < n || return nothing
    0 < i <= n || throw(BoundsError(tv, i))
    @inbounds wait(tv[i])
end

"""
    fetch(tl::TaskChain)

Wait for last Task in to finish, then return its result value.
If the task fails with an exception, a `TaskFailedException`
(which wraps the failed task) is thrown.
"""
function fetch(tv::TaskChain, ix::Integer=0)
    n = length(tv)
    i = ix == 0 ? n : Int(ix)
    0 < n || throw(ArgumentError("cannot fetch from empty task list"))
    0 < i <= n || throw(BoundsError(tv, i))
    @inbounds fetch(tv[i])
end

function Base.kill(bc::BTask{T,<:Process}, signum=Base.SIGTERM) where T
    kill(bc.task, signum)
end
function Base.kill(bc::BTask{T,<:Task}, signum=Base.SIGTERM) where T
    ex = ErrorException("Task $bc was killed($signum)")
    schedule(bc.task, ex, error=true)
    yield()
end

function Base.kill(tl::TaskChain)
    x = findfirst(t->istaskstarted(t) && !istaskdone(t), tl.processes)
    x !== nothing && kill(tl[x])
    nothing
end

function show(io::IO, m::MIME"text/plain", tv::TaskChain)
    for t in tv.processes
        show(io, m, t)
        t !== last(tv.processes) && println(io)
    end
end

length(tv::TaskChain) = length(tv.processes)
getindex(tv::TaskChain, i::Integer) = getindex(tv.processes, i)

#= iteration and broadcasting delegated to .processes
iterate(tv::TaskChain, s...) = iterate(tv.processes, s...)
broadcastable(tv::TaskChain) = tv.processes
lastindex(tv::TaskChain) = lastindex(tv.processes)
firstindex(tv::TaskChain) = firstindex(tv.processes)
=#

istaskdone(tv::TaskChain) = all(istaskdone.(tv.processes))
istaskfailed(tv::TaskChain) = any(istaskfailed.(tv.processes))
istaskstarted(tv::TaskChain) = all(istaskstarted.(tv.processes))

"""
    task_code, task_cin, task_cout, task_function, task_args

Access the argumentless function provided to the task
"""
task_code(t::BTask{:Task}) = t.task.code
task_code(t::BTask{:Threat}) = t.task.code.task_function
task_cin(t::BTask) = task_code(t).cin
task_cout(t::BTask) = task_code(t).cout
function task_function(t::BTask)
    bc = task_code(t).bc
    eval(bc.m).eval(bc.f)
end
task_args(t::BTask) = task_code(t).bc.args

local_reader(chd::RemoteChannelIODescriptor) = ChannelIO(chd, :R)
local_writer(chd::RemoteChannelIODescriptor) = ChannelIO(chd, :W)
local_reader(cio::AbstractPipe) = pipe_reader2(cio)
local_writer(cio::AbstractPipe) = pipe_writer2(cio)
local_reader(cio::ChannelIO) = pipe_reader2(cio)
local_writer(cio::ChannelIO) = pipe_writer2(cio)
local_reader(cio) = cio
local_writer(cio) = cio

remote_bridge(chd::RemoteChannelIODescriptor) = chd
remote_bridge(::Any) = DEFAULT_IN

pipe_reader2(cio::AbstractPipe) = pipe_reader2(pipe_reader(cio))
pipe_reader2(cio::IO) = cio
pipe_reader2(::Any) = DEFAULT_IN
pipe_writer2(cio::AbstractPipe) = pipe_writer2(pipe_writer(cio))
pipe_writer2(cio::IO) = cio
pipe_writer2(::Any) = DEFAULT_OUT

"""
    run(BClosure; stdin=devnull, stdout=devnull, wait=true)

Start parallel task redirecting stdin and stdout.
"""
function run(bt::BClosure; stdin=DEFAULT_IN, stdout=DEFAULT_OUT, wait::Bool=true)
    run(BClosureList([bt]); stdin, stdout, wait)
end
"""
    run(RClosure; stdin=devnull, stdout=devnull, wait=true)

Start parallel remote task, redirect remote channels if available.
"""
function run(bt::RClosure; stdin=DEFAULT_IN, stdout=DEFAULT_OUT, wait::Bool=true)
    bcl = bt.bcl
    cin = remote_bridge(bcl.cin)
    cout = remote_bridge(bcl.cout)
    run(BClosureList([bt], cin, cout); stdin, stdout, wait)
end

"""
    run(::BClosureList; stdin=devnull, stdout=devnull, wait=true)

Start all parallel tasks defined in list, io redirection falls back to BClosureList values.
"""

function run(bcl::BClosureList; stdin=DEFAULT_IN, stdout=DEFAULT_OUT, wait::Bool=true)
    tv, cin0, cout0 = _run(bcl, stdin, stdout, false, false)
    tl = TaskChain(BTask{task_thread()}.(tv), pipe_writer2(cin0), pipe_reader2(cout0))
    if wait
        Base.wait(tl)
    end
    tl
end

function _run(bcl::BClosureList, stdi, stdo, pin::Bool, pout::Bool)
    list = bcl.list
    n = length(list)
    tv = Vector{Union{Task,AbstractPipe,Future,TaskChainProxy}}(undef, n)
    io = Vector{AllIO}(undef, n+1)
    fill!(io, devnull)
    cin, cout = overrideio(stdi, stdo, bcl)
    io[1], io[n+1] = cin, cout
    # start `AbstractCmd`s first to obtain their io endpoints
    for i = 1:n
        s = list[i]
        if s isa AbstractCmd
            xio = i == 1 ? io[1] : i == n ? io[n+1] : devnull
            write = i > 1 || xio == devnull
            read = i < n || xio == devnull
            pipe = open(s, xio; write, read)
            tv[i] = pipe
            if write
                if io[i] === devnull
                    io[i] = pipe
                else
                    throw_missing_adapter(s, :before, io[i])
                end
            end
            if read
                if io[i+1] === devnull
                    io[i+1] = pipe
                else
                    throw_missing_adapter(s, :after, io[i+1])
                end
            end
        elseif s isa RClosure
            if io[i] === devnull
                io[i] = RemoteChannelIODescriptor(myid())
            elseif !(io[i] isa RemoteChannelIODescriptor)
                throw_missing_adapter(s, :before, io[i])
            end
            if io[i+1] === devnull
                io[i+1] = RemoteChannelIODescriptor(s.id)
            elseif !(io[i+1] isa RemoteChannelIODescriptor)
                throw_missing_adapter(s, :after, io[i+1])
            end
        else #if s isa BClosure
            if (pin || i > 1) && io[i] === devnull
                io[i] = ChannelPipe()
            end
        end
    end
    if pout && io[n+1] === devnull
        io[n+1] = ChannelPipe()
    end
    for i = 1:n
        s = list[i]
        if !(s isa AbstractCmd)
            cin = io[i]
            cout = io[i+1]
            tv[i] = _schedule(s, cin, cout)
        end
    end
    return tv, local_writer(io[1]), local_reader(io[n+1])
end

# prefer arguments, but use bcl-values in default case
function overrideio(stdi, stdo, bcl)
    cin = (stdi === DEFAULT_IN ? bcl.cin : stdi)
    cout = (stdo === DEFAULT_OUT ? bcl.cout : stdo)
    cin, cout
end

@noinline function throw_missing_adapter(task, ba, io)
    throw(ArgumentError("$ba $task no $io is possible."))
end

function readwrite_from_mode(mode::AbstractString)
    read = mode == "r" || mode == "w+" || mode == "r+"
    write = mode == "w" || mode == "w+" || mode == "r+"
    read, write
end

function open(bt::BRClosureList, mode::AbstractString, stdio::Redirectable=devnull)
    read, write = readwrite_from_mode(mode)
    open(bt, stdio; read, write)
end
function open(f::Function, bt::BRClosureList, mode::AbstractString, stdio::Redirectable=devnull)
    read, write = readwrite_from_mode(mode)
    open(f, bt, stdio; read, write)
end

function open(cmds::BClosureList, stdio::Redirectable=devnull; write::Bool=false, read::Bool=!write)

    if read && write && stdio != devnull
        throw(ArgumentError("no stream can be specified for `stdio` in read-write mode"))
    end
    cout = read ? devnull : stdio == devnull ? cmds.cout : stdio
    cin = write ? devnull : stdio == devnull ? cmds.cin : stdio
    tv, cin0, cout0 = _run(cmds, cin, cout, write, read)
    cin0 = write ? cin0 : devnull
    cout0 = read ? cout0 : devnull
    TaskChain(BTask{task_thread()}.(tv), local_writer(cin0), local_reader(cout0))
end

function open(bt::BClosure, stdio::Redirectable=devnull; write::Bool=false, read::Bool=!write)
    open(BClosureList([bt]), stdio; read, write)
end
function open(f::Function, bt::BClosure, stdio::Redirectable=devnull; write::Bool=false, read::Bool=!write)
    open(f, BClosureList([bt]), stdio; read, write)
end

"""

    closure(f::Function, args...)

Generate a `BClosure` object, which can be used to be started in parallel.
The function `f` must have the signature `f(cin::IO, cout::IO [, args...])`.
It may be wrapped in an argumentless closure to be used in a `Task` definition.
"""
function closure(f::Function, args...)
    BClosure(f, args)
end

use_tasks() = Threads.nthreads() <= 1 || Threads.threadid() != 1
task_thread() = use_tasks() ? :Task : :Threat

# schedule single task
function _schedule(bc::BClosure, cin, cout)

    output("called _schedule(@$(myid()), $bc, $cin, $cout)")
    cin = local_reader(cin)
    cout = local_writer(cout)
    function task_function()
        ci = vopen(cin, false)
        co = vopen(cout, true)
        try
            f = eval(bc.m).eval(bc.f)
            f(ci, co, bc.args...)
        catch
            #= print stack trace to stderr
            for (exc, bt) in current_exceptions()
                showerror(stderr, exc, bt)
                println(stderr)
            end
            =#
            rethrow()
        finally
            vclose(cin, ci)
            vclose(cout, co)
        end
    end
    if use_tasks()
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end

vopen(file::AbstractString, write::Bool) = open(file; write)
vopen(fr::FileRedirect, write::Bool) = open(fr.filename; write, append=fr.append)
vopen(cio::Any, ::Bool) = cio

vclose(cio, handle) = cio != handle ? close(handle) : vclose(handle)

# vclose(::TTY) = nothing # covered by IO must not be changed to avoid REPL kill
vclose(cio::ChannelIO) = close(cio)
vclose(cio::Base.PipeEndpoint) = close(cio)
vclose(::IO) = nothing

pipe_reader(tio::TaskChain) = tio.out
pipe_writer(tio::TaskChain) = tio.in
