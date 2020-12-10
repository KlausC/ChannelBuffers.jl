module ChannelBuffers

export ChannelIO, ChannelPipe, →
export reverseof

# debugging support
const LOCK = ReentrantLock()
const DEBUGINFO = Ref(false)
function dprintln(args...)
    if DEBUGINFO[]
        lock(LOCK) do
            println(args...)
        end
    end
end

include("channelio.jl")
include("tasks.jl")
include("specialtasks.jl")

end # module
