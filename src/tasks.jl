
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
    
    closure(f::Function, args...)

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
    if use_tasks()
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
vclose(cio, write) = nothing

function noop()
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
    closure(_noop)
end

noop_eof(ci::IO) = eof(ci)
noop_read(ci::IO, b) = b[1:readbytes!(ci, b)]
noop_write(co::IO, x) = write(co, x)

noop_eof(ch::Channel) = isempty(ch) && !isopen(ch)
noop_read(ch::Channel) = take!(ch)
noop_write(ch::Channel, x) = put!(ch, x)

noop_eof(ci::ChannelIO) = noop_eof(ci.ch)
function noop_read(ci::ChannelIO, b)
    x = noop_read(ci.ch)
    ci.position += sizeof(x)
    x
end
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
