
struct BTask{X,T}
    task::T
    BTask{X}(t::T) where {X,T} = new{X,T}(t)
end
show(io::IO, m::MIME"text/plain", bt::BTask) = show(io, m, bt.task)
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

function Base.kill(bc::BTask, ex::Exception=ErrorException("Task $bc was killed"))
    schedule(bc.task, ex, error=true)
    yield()
end

# List of tasks and io redirections - output of `run` and `open`
"""
    TaskChain

An `AbstractPipe` wich contains a list of running tasks and can be used for
reading and writing. Created by a call to `[open|run](::BClosureList)`.

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
If the task fails with an exception, a `TaskFailedException` (which wraps the failed task) is thrown.
"""
function fetch(tv::TaskChain, ix::Integer=0)
    n = length(tv)
    i = ix == 0 ? n : Int(ix)
    0 < n || throw(ArgumentError("cannot fetch from empty task list"))
    0 < i <= n || throw(BoundsError(tv, i))
    @inbounds fetch(tv[i])
end

function Base.kill(tl::TaskChain)
    x = 1
    while (x = findnext(!istaskdone, tl.processes, x)) !== nothing
        try
            kill(tl[x])
            break
        catch
            nothing
        end
    end
    nothing
end

"""
    close(::TaskChain)

First close pipe_writer (the device the pipe is reading from).
That should flush all pending data, giving an EOF to the fist task which should exit.
Then wait for the last task to be done.
The pipe_reader would be closed automatically before the completion of the last task.
"""
function close(tv::TaskChain)
    close(pipe_writer(tv))
    close(pipe_reader(tv))
end

function show(io::IO, m::MIME"text/plain", tv::TaskChain)
    for t in tv.processes
        show(io, m, t)
        println(io)
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
task_function(t::BTask) = task_code(t).btd.f
task_args(t::BTask) = task_code(t).btd.args

to_pipe(cio::ChannelIO) = ChannelPipe(cio)
to_pipe(cio) = cio
pipe_reader2(cio::AbstractPipe) = pipe_reader2(pipe_reader(cio))
pipe_reader2(cio::IO) = cio
pipe_reader2(cio) = DEFAULT_OUT
pipe_writer2(cio::AbstractPipe) = pipe_writer2(pipe_writer(cio))
pipe_writer2(cio::IO) = cio
pipe_writer2(cio) = DEFAULT_IN

"""
    run(BClosure; stdin=devnull, stdout=devnull, wait=true)

Start parallel task redirecting stdin and stdout
"""
function run(bt::BClosure; stdin=DEFAULT_IN, stdout=DEFAULT_OUT, wait::Bool=true)
    run(BClosureList([bt]); stdin, stdout, wait)
end

"""
    run(::BClosureList; stdin=devnull, stdout=devnull, wait=true)

Start all parallel tasks defined in list, io redirection defaults are defined in the list
"""
function _run(btdl::BClosureList, stdin=DEFAULT_IN, stdout=DEFAULT_OUT)
    list = btdl.list
    n = length(list)
    tv = Vector{Union{Task,AbstractPipe}}(undef, n)
    cin0 = stdin === DEFAULT_IN ? btdl.cin : stdin
    cout0 = stdout === DEFAULT_OUT ? btdl.cout : stdout
    cout = to_pipe(cout0)
    i = n
    # start tasks in reverse order of list
    while i > 1
        s = list[i]
        if s isa AbstractCmd
            cout = open(s, write=true, read= i == n)
            tv[i] = cout
            i -= 1
        else
            if list[i-1] isa AbstractCmd
                cin = open(list[i-1], read=true, write = i > 2)
                tv[i-1] = cin
                di = 2
            else
                cin = ChannelPipe()
                di = 1
            end
            tv[i] = _schedule(s, cin, cout)
            cout = cin
            i -= di
        end
    end
    if i >= 1
        s = list[i]
        if s isa AbstractCmd
            tv[i] = open(s, write=true, read= i == n)
        else
            tv[i] = _schedule(s, to_pipe(cin0), cout)
        end
    end
    return tv, cin0, cout0
end

function run(btdl::BClosureList; stdin=DEFAULT_IN, stdout=DEFAULT_OUT, wait::Bool=true)
    tv, cin0, cout0 = _run(btdl, stdin, stdout)
    tl = TaskChain(BTask{task_thread()}.(tv), pipe_writer2(cin0), pipe_reader2(cout0))
    if wait
        Base.wait(tl)
    end
    tl
end

function readwrite_from_mode(mode::AbstractString)
    read = mode == "r" || mode == "w+" || mode == "r+"
    write = mode == "w" || mode == "w+" || mode == "r+"
    read, write
end

function open(bt::BClosureAndList, mode::AbstractString, stdio::Redirectable=devnull)
    read, write = readwrite_from_mode(mode)
    open(bt, stdio; read, write)
end
function open(f::Function, bt::BClosureAndList, mode::AbstractString, stdio::Redirectable=devnull)
    read, write = readwrite_from_mode(mode)
    open(f, bt, stdio; read, write)
end

function open(cmds::BClosureList, stdio::Redirectable=devnull; write::Bool=false, read::Bool=!write)

    if read && write && stdio != devnull
        throw(ArgumentError("no stream can be specified for `stdio` in read-write mode"))
    end
    cout = read ? ChannelIO(R) : stdio == devnull ? cmds.cout : stdio
    cin = write ? ChannelIO(W) : stdio == devnull ? cmds.cin : stdio
    tv, cin0, cout0 = _run(pipeline(cin, cmds.list..., cout))
    cin0 = write ? cin0 : devnull
    cout0 = read ? cout0 : devnull
    TaskChain(BTask{task_thread()}.(tv), pipe_writer2(cin0), pipe_reader2(cout0))
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
function _schedule(btd::BClosure, cin, cout)
    function task_function()
        ci = vopen(cin, false)
        co = vopen(cout, true)
        try
            btd.f(ci, co, btd.args...)
        catch
            #= print stack trace to stderr
            for (exc, bt) in current_exceptions()
                showerror(stderr, exc, bt)
                println(stderr)
            end
            =#
            rethrow()
        finally
            vclose(here(cin), ci, false)
            vclose(here(cout), co, true)
        end
    end
    if use_tasks()
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end

here(::Union{AbstractString,FileRedirect}) = true
here(::Any) = false
vopen(file::AbstractString, write::Bool) = open(file; write)
vopen(fr::FileRedirect, write::Bool) = open(fr.filename; write=write, append=fr.append)
vopen(cio::Any, ::Bool) = cio

vclose(here::Bool, cio, write::Bool) = here ? close(cio) : vclose(cio, write)

vclose(::TTY, write::Bool) = nothing # must not be changed to avoid REPL kill
vclose(cio::IOContext, write::Bool) = vclose(cio.io, write)
function vclose(cio::AbstractPipe, write::Bool)
    w = pipe_writer(cio)
    write && isopen(w) ? close(w) : nothing
end
vclose(cio::ChannelIO, ::Bool) = close(cio)
vclose(cio::ChannelPipe, write::Bool) = vclose(pipe_writer(cio), write)
vclose(::IO, ::Bool) = nothing

# NOOP task - copy cin to cout
function noop()
    function _noop(cin::IO, cout::IO)
        while !eof(cin)
            write(cout, read(cin))
        end
    end
    closure(_noop)
end

"""
    const NOOP

A BClosure which copies input to output unmodified.
"""
NOOP = noop()

pipe_reader(tio::TaskChain) = tio.out
pipe_writer(tio::TaskChain) = tio.in
