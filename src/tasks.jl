
"""
    BClosure(f::function, args)

Store function and arguments. The signature of the function must
be like `f(cin::IO, cout::IO, args...)`.
"""
struct BClosure{F<:Function,Args<:Tuple}
    f::F
    args::Args
end

# used for output redirection
const UIO = Union{IO,AbstractString}

# List of BClosure objects an io redirections
struct BClosureList{In,Out}
    list::Vector{<:BClosure}
    cin::In
    cout::Out
    BClosureList(list, cin::In, cout::Out) where {In,Out} = new{In,Out}(list, cin, cout)
end
BClosureList(list) = BClosureList(list, stdin, stdout)

struct BTask{X}
    task::Task
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

import Base: |, <, >
|(src::BClosureList, btd::BClosure) = BClosureList(vcat(src.list, btd), src.cin, src.cout)
|(src::BClosure, btd::BClosure) = BClosureList([src, btd])

<(list::BClosureList, cin::IO) = BClosureList(list.list, cin, list.cout)
<(list::BClosure, cin::IO) = BClosureList([list], cin, stdout)

>(list::BClosureList, cout::IO) = BClosureList(list.list, list.cin, cout)
>(list::BClosure, cout::IO) = BClosureList([list], stdin, cout)

function Base.pipeline(src::BClosure, other::BClosure...; stdin=stdin, stdout=stdout)
    BClosureList([src; other...], stdin, stdout)
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
Base.show(io::IO, m::MIME"text/plain", tv::BTaskList) = show(io, m, tv.list)

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
    run(BClosure; stdin=stdin, stdout=stdout)

Start parallel task redirecting stdin and stdout
"""
function Base.run(bt::BClosure; stdin=stdin, stdout=stdout)
    run(BClosureList([bt], stdin, stdout))
end

"""
    run(::BClosureList)

Start all parallel tasks defined in list, io redirection defaults are defined in the list
"""
function Base.run(btdl::BClosureList)
    T = use_tasks() ? :Task : :Threat
    stdin = btdl.cin
    stdout = btdl.cout
    n = length(btdl.list)
    tl = Vector{Task}(undef, n)
    s = btdl.list[n]
    cout = stdout
    cin = n == 1 ? stdin : ChannelIO()
    t = _schedule(s, cin, cout)
    tl[n] = t
    for i = n-1:-1:1
        s = btdl.list[i]
        cout = reverseof(cin)
        cin = i == 1 ? stdin : ChannelIO()
        t = _schedule(s, cin, cout)
        tl[i] = t
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
        try
            btd.f(cin, cout, btd.args...)
        finally
            cout isa ChannelIO && close(cout)
        end
    end
    if Threads.nthreads() <= 1 || Threads.threadid() != 1
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end
