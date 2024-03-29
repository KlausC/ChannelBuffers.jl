module ChannelBuffers

export ChannelIO, ChannelPipe, →
export reverseof, noop

using Base: Process, AbstractPipe, AbstractCmd, FileRedirect, TTY
import Base: pipe_reader, pipe_writer
import Base: run, open, close, wait, fetch, eof, read, write, flush
import Base: show, length, getindex
import Base: unsafe_write, bytesavailable, readbytes!, unsafe_read, pipeline
import Base: isopen, isreadable, iswritable, position, seek, skip, peek, take!
import Base: iterate, broadcastable, lastindex, firstindex
import Base: istaskstarted, istaskdone, istaskfailed
import Base: |

include("channelio.jl")
include("pipelines.jl")
include("tasks.jl")
include("specialtasks.jl")

end # module
