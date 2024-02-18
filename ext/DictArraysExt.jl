module DictArraysExt

using DictArrays
using VirtualObservatory: VizierCatalog, TAPTable, VOTables, execute

DictArrays.DictArray(c::VizierCatalog; kwargs...) = VOTables.read(DictArray, download(c); c.unitful, kwargs...)
DictArrays.DictArray(t::TAPTable; kwargs...) = execute(DictArray, t.service, "select * from \"$(t.tablename)\""; kwargs...)

end
