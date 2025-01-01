module VirtualObservatory

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

export
    VOService,
    TAPService, TAPTable,
    DataLinkService, datalink_table,
    VizierCatalog,
    table, execute, Cols, All, metadata, colmetadata

include("TAP.jl")
include("datalink.jl")
include("vizier.jl")


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
