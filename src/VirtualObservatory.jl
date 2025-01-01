@doc Base.read(joinpath(dirname(@__DIR__), "README.md"), String) module VirtualObservatory

using VOTables
import Tables: table
using CURL_jll
using DataPipes
using AccessorsExtra
using FlexiMaps
using StructArrays
using CSV
using URIs
import DBInterface: connect, execute
using DataAPI: Cols, All

export TAPService, TAPTable, VizierCatalog, table, execute, Cols, All, metadata, colmetadata

include("TAP.jl")
include("vizier.jl")


# XXX: piracy
# see https://github.com/JuliaWeb/URIs.jl/pull/55
# XXX: should be just this, but tests won't pass then...
# Base.download(uri::URI, args...) = download(URIs.uristring(uri), args...)
Base.download(uri::URI, file=tempname()) = 
    try
        download(URIs.uristring(uri), file)
    catch e
        try
            run(`$(curl()) --compressed $(URIs.uristring(uri)) --output $file --insecure`)
            file
        catch
            run(`curl --compressed $(URIs.uristring(uri)) --output $file --insecure`)
            file
        end
    end
end
