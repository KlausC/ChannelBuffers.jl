module ChannelBuffers

export ChannelIO, ChannelPipe, →
export reverseof

# debugging support
const LOCK = ReentrantLock()
const DEBUGINFO = Ref(false)
const LOG = "/tmp/channel.log"
function dprintln(args...)
    if DEBUGINFO[]
        lock(LOCK) do
            logfile = open(LOG, append=true)
            write(logfile, args...)
            write(logfile, '\n')
            flush(logfile)
        end
    end
end

include("channelio.jl")
include("tasks.jl")
include("specialtasks.jl")

end # module
