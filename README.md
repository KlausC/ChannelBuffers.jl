# ChannelBuffers

[![Build Status](https://travis-ci.com/KlausC/ChannelBuffers.jl.svg?branch=master)](https://travis-ci.com/KlausC/ChannelBuffers.jl)
[![Codecov](https://codecov.io/gh/KlausC/ChannelBuffers.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/KlausC/ChannelBuffers.jl)

The `ChannelBuffers` package allows to perform parallel processing within `Julia` in the form of commandline pipes.
If the user provides functions `f`, `g`, `h`, of the form
`f(input::IO, output::IO, args...)`, which read from in input stream and write their
results to an output stream, they can execute the functions in parallel tasks.

## Examples

``` julia
    tl = run(closure(f, fargs...) | closure(g, gargs...) < "afile" > "bfile")
    wait(tl)
```

Some standard closures are predefined, which make that possible:

``` julia
    tl = run( curl("https::/myurltodownloadfrom.tgz") | gunzip() | tarx("targetdir") )
```

or

``` julia
    a = my_object
    run( serializer(a) > "xfile") |> wait
    b = run(deserializer() < "xfile") |> fetch
```

## Predefined closures

``` julia
    tarx(dir) - read files in input directory and write to output stream
    tarc(dir) - read input stream and create files in target directory
    gzip() - read input stream and write compressed data to output stream
    gunzip() - reverse of gzip
    transcoder(::Codec) - generalization for other kinds of `TranscoderStreams`
    curl(URL) - download file from URL and write to output stream
    serializer(obj) - write serialized for of input object to output stream
    deserializer() - read input stream and reconstruct serialized object
```

## Implementation

The internal pipes are implemented by `ChannelIO <: IO` which uses `Channel` objects to transport data between tasks.
The tasks are spawned on different threads, if multithreading is available (`JULIA_NUM_THREADS > 1`).

## API

To create a user defined task, a function with the signature `f(cin::IO, cout::IO, args...)` is required.
It can be transformed into a `BClosure` object

``` julia
        fc = closure(f, args...)::BClosure
```

which can be run alone or combined with other closures and input/output specifiers.
The following `Base` functions are redefined.

``` julia
        Base: |, < , >, run, pipeline, wait, fetch
```

which are used as in

``` julia
    tl = run(fc::BClosure)::BTaskList

    pl = fc | gc | hc > out < in
    pl = pipeline(fc, gc, hc, stdin=in stdout=out)::BClosureList

    tl = run(pl::BClosureList)::BTaskList
```

The assignments to `pl` are equivalent.

The pipelined tasks are considered finished, when the statically last task in the list terminates.
The calling task can wait for this event with

``` julia
    wait(tl::BTaskList)::Nothing
```

If the last task in the pipeline calculates a value, if can be waited for and obtained by

``` julia
    fetch(tl::BTaskList)::Any
```

Both `wait` and `fetch` throw `TaskFailedException` if the last task in the list failed.

Element type of `BTaskList` is `BTask`, a tagging wrapper around `Task`. It delegates the most important
methods, like `wait`, `fetch`, `istask...`.
