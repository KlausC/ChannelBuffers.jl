
using Distributed

"""
    localchannel(::RemoteChannel)

FOr a remote channel which is located at your own process id
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

@noinline function throw_notlocal(rc)
    throw(ArgumentError("$rc is not local on process $(myid())."))
end
