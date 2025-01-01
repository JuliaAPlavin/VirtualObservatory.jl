module VirtualObservatory

using VOTables
import Tables: table
using CURL_jll
using DataPipes
using AccessorsExtra
using FlexiMaps
using StructArrays
using CSV
using HTTP
using URIs
import DBInterface: connect, execute
using DataAPI: Cols, All

export
    VOService,
    TAPService, TAPTable,
    DataLinkService, datalink_table,
    VizierCatalog,
    table, execute, Cols, All, metadata, colmetadata

include("TAP.jl")
include("datalink.jl")
include("vizier.jl")

"""
`HTTP_REQUEST_OPTIONS` is a `Dict{Symbol,Any}` that can be used to specify
additional keyword arguments/values to be passed to `HTTP.request`.  Any of the
keyword arguments supported by `HTTP.request` may be used, but `query` and
`response_stream` will be ignored as they are specified internally.

Currently, only `execute` for `TAPService` uses `HTTP.jl` so options specified
here will not influence other service types.

Hint: To match previous behavior, `VirtualObservatory` uses
`require_ssl_verification=false`, but for additional security you can use:

    HTTP_REQUEST_OPTIONS[:require_ssl_verification] = true
"""
const HTTP_REQUEST_OPTIONS = Dict{Symbol, Any}()

struct VOService
    tap::Union{Nothing,TAPService}
    datalink::Union{Nothing,DataLinkService}
end

VOService(service::Symbol) = VOService(
    TAPService(service),
    DataLinkService(service),
)

execute(svc::VOService, args...; kwargs...) = execute(svc.tap, args...; kwargs...)
execute(T::Type, svc::VOService, args...; kwargs...) = execute(T, svc.tap, args...; kwargs...)

datalink_table(svc::VOService, args...; kwargs...) = datalink_table(svc.datalink, args...; kwargs...)

end
