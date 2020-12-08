# ChannelBuffers

[![Build Status][gha-img]][gha-url]     [![Coverage Status][codecov-img]][codecov-url]

## Introduction

The `ChannelBuffers` package allows to perform parallel processing within `Julia` in the form of commandline pipes.
If the user provides functions `f`, `g`, `h`, of the form
`f(input::IO, output::IO, args...)`, which read from in input stream and write their
results to an output stream, they can execute the functions in parallel tasks.

**Note:**

Input/Output redirection is denoted by `→` (`\rightarrow`), which indicates the direction of data flow.
Besides that we support `|` to denote task pipelines. The symbols `<` and `>` known from commandline shells cannot be used,
because they bear the semantics of comparison operators in `Julia`.

## Examples

``` julia

    tl = run("afile" → closure(f, fargs...) → closure(g, gargs...) → "bfile")
    wait(tl)
    
```

Some standard closures are predefined, which make that possible:

``` julia
    tl = run( curl("https::/myurltodownloadfrom.tgz") | gunzip() | tarx("targetdir") )
```

or

``` julia
    a = my_object
    run( serializer(a) → "file") |> wait

    b = open("file") do cin
        run(cin → deserializer()) |> fetch
    end
```

## Predefined closures

``` julia
    tarx(dir) # read files in input directory and write to output stream
    tarc(dir) # read input stream and create files in target directory
    gzip() # read input stream and write compressed data to output stream
    gunzip() # reverse of gzip
    transcoder(::Codec) # generalization for other kinds of TranscoderStreams
    curl(URL) # download file from URL and write to output stream
    serializer(obj) # write serialized for of input object to output stream
    deserializer() # read input stream and reconstruct serialized object
```

## API

To create a user defined task, a function with the signature `f(cin::IO, cout::IO, args...)` is required.
It can be transformed into a `BClosure` object

``` julia
        fc = closure(f, args...)::BClosure
```

which can be run alone or combined with other closures and input/output specifiers.
The following `Base` functions are redefined.

``` julia
        Base: |, run, pipeline, wait, fetch
```

which are used as in

``` julia
    tl = run(fc::BClosure)::BTaskList

    pl = in → fc → gc → hc → out
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

## Implementation

The internal pipes are implemented by `ChannelIO <: IO` which uses `Channel` objects to transport data between tasks.
The tasks are spawned on different threads, if multithreading is available (`JULIA_NUM_THREADS > 1`).
Communication endpoints of the pipeline can be arbitrary `IO` objects or `AbstractString`s denoting file names.
The files given as strings are appropriately opened and closed.

Element type of `BTaskList` is `BTask`, a tagging wrapper around `Task`. It delegates the most important
methods, like `wait`, `fetch`, `istask...`.

[gha-img]: https://github.com/KlausC/ChannelBuffers.jl/workflows/CI/badge.svg
[gha-url]: https://github.com/KlausC/ChannelBuffers.jl/actions?query=workflow%3ACI

[coveral-img]: https://coveralls.io/repos/github/KlausC/ChannelBuffers.jl/badge.svg?branch=master
[coveral-url]: https://coveralls.io/github/KlausC/ChannelBuffers.jl?branch=master
[codecov-img]: https://codecov.io/gh/KlausC/ChannelBuffers.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/KlausC/ChannelBuffers.jl
