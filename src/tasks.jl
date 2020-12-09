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
const DEFAULT_IN = devnull
const DEFAULT_OUT = devnull

# List of BClosure objects an io redirections
struct BClosureList{In,Out}
    list::Vector{<:BClosure}
    cin::In
    cout::Out
    BClosureList(list, cin::In, cout::Out) where {In,Out} = new{In,Out}(list, cin, cout)
end
BClosureList(list) = BClosureList(list, DEFAULT_IN, DEFAULT_OUT)

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

import Base: |

|(left::Union{BClosure,BClosureList}, right::Union{BClosure,BClosureList}) = →(left, right)
→(left::BClosureList, right::BClosureList) = BClosureList(vcat(left.list, right.list), left.cin, right.cout)
→(left::BClosureList, right::BClosure) = BClosureList(vcat(left.list, right), left.cin, left.cout)
→(left::BClosure, right::BClosureList) = BClosureList(vcat(left, right.list), right.cin, right.cout)
→(left::BClosure, right::BClosure) = BClosureList([left, right])

→(cin::UIO, list::BClosureList) = BClosureList(list.list, cin, list.cout)
→(cin::UIO, list::BClosure) = BClosureList([list], cin, DEFAULT_OUT)

→(list::BClosureList, cout::UIO) = BClosureList(list.list, list.cin, cout)
→(list::BClosure, cout::UIO) = BClosureList([list], DEFAULT_IN, cout)

function Base.pipeline(src::BClosure, other::BClosure...; stdin=DEFAULT_IN, stdout=DEFAULT_OUT)
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
    n = length(btdl.list)
    tl = Vector{Task}(undef, n)
    cout = btdl.cout
    for i = n:-1:2
        cp = ChannelPipe()
        s = btdl.list[i]
        tl[i] = _schedule(s, cp.out, cout)
        cout = cp.in
    end
    if n >= 1
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
        ci = vopen(cin, "r")
        co = vopen(cout, "w")
        try
            btd.f(ci, co, btd.args...)
        finally
            vclose(ci, cin, "r")
            vclose(co, cout, "w")
        end
    end
    if Threads.nthreads() <= 1 || Threads.threadid() != 1
        schedule(Task(task_function))
    else
        Threads.@spawn task_function()
    end
end

vopen(file::AbstractString, mode::AbstractString) = open(file, mode)
vopen(cio::IO, mode::AbstractString) = cio
vclose(cio::IO, file::AbstractString, mode::AbstractString) = close(cio)
vclose(cio::ChannelIO, ::Any, mode::AbstractString) = mode == "w" ? close(cio) : nothing
vclose(cio::Base.TTY, ::Any, mode::AbstractString) = nothing # must not be changed to avoid REPL kill
vclose(cio::IO, ::Any, mode::AbstractString) = nothing
