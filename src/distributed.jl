
using Distributed

OUTPUT = [] # RemoteChannel(() -> Channel(1000), 1)
output(s::Any) = push!(OUTPUT, s)

TASKLISTS = Dict{Any,Any}()

mutable struct TaskChainProxy
    id::Int
    reference::UInt
    function TaskChainProxy(tc::TaskChain)
        ref = objectid(tc)
        x = new(myid(), ref)
        TASKLISTS[ref] = (proxy=x, tc=tc)
        finalizer(fin, x)
    end
end

function fin(tr::TaskChainProxy)
    if myid() == tr.id
        delete!(TASKLISTS, tr.reference)
    else
        remotecall_fetch(tr.id, tr.reference) do ref
            delete!(ChannelBuffers.TASKLISTS, ref)
        end
    end
    nothing
end

function show(io::IO, m::MIME"text/plain", bt::BTask{T,<:TaskChainProxy} where T)
    tp = bt.task
    rs = remotecall_fetch(tp.id, tp.reference) do ref
        tl = get(TASKLISTS, ref, nothing)
        tl === nothing ? "@$(tp.id) TaskChain   @$(repr(ref))" : sprint(show, m, tl.tc)
    end
    print(io, rs)
end

"""
    localchannel(::RemoteChannel)

For a remote channel which is located at your own process id
find the implementing `Channel` object.
"""
function localchannel(rc::RemoteChannel)
    rc.where == myid() || throw_notlocal(rc)
    rid = Distributed.remoteref_id(rc)
    Distributed.lookup_ref(rid).c
end

function channel_length(rc::RemoteChannel)
    if rc.where == myid()
        channel_length(localchannel(rc))
    else
        remotecall_fetch(channel_length, rc.where, rc)
    end
end

# schedule task at remote process
function _schedule(rc::RClosure, cin, cout)

    output("called _schedule($rc, $cin, $cout)")
    remotecall_fetch(remoterun, rc.id, rc.bcl, cin, cout)
end

function remoterun(bcl::BClosureList, stdi, stdo)
    cin, cout = overrideio(stdi, stdo, bcl)
    tv, cr, cw = _run(bcl, cin, cout, false, false)
    tl = TaskChain(BTask{task_thread()}.(tv), cr, cw)
    # return sprint(show, MIME"text/plain"(), tl) # TODO return serializable task proxy
    TaskChainProxy(tl)
end

@noinline function throw_notlocal(rc)
    throw(ArgumentError("$rc is not local on process $(myid())."))
end
