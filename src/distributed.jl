
using Distributed

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

# schecule task at remote process
function _schedule(rc::RClosure, cin, cout)
    remotecall(remoterun, rc.id, rc.bcl, cin, cout)
end

function remoterun(bcl::BClosureList, stdin, stdout)
    cin, cout = overrideio(stdin, stdout, bcl)
    tv, cr, cw = _run(bcl, cin, cout)
    tl = TaskChain(BTask{task_thread()}.(tv), cr, cw)
    return sprint(show, MIME"text/plain"(), tl) # TODO return serializable task proxy
end

@noinline function throw_notlocal(rc)
    throw(ArgumentError("$rc is not local on process $(myid())."))
end
