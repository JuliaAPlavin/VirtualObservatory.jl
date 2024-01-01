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

export TAPService, VizierCatalog, table, execute


_TAP_SERVICE_URLS = Dict(
	:vizier => "http://tapvizier.cds.unistra.fr/TAPVizieR/tap",
)

"""
    TAPService(baseurl)
    TAPService(service::Symbol)

Handler of a service following the Virtual Observatory Table Access Protocol (TAP), as [defined](https://www.ivoa.net/documents/TAP/) by IVOA.
Instances of `TAPService` can be created by passing either a base URL of the service or a symbol corresponding to a known service: $(@p keys(_TAP_SERVICE_URLS) |> collect |> sort |> map("`:$_`") |> join(__, ", ")).

A `TAPService` aims to follow the `DBInterface` interface, with query execution as the main feature: `execute(tap, query::String)`.
"""
struct TAPService
	baseurl::URI
end

TAPService(baseurl::AbstractString) = TAPService(URI(baseurl))
TAPService(service::Symbol) = TAPService(_TAP_SERVICE_URLS[service])

connect(::Type{TAPService}, args...) = TAPService(args...)

"""    execute(tap::TAPService, query::AbstractString; kwargs...)

Execute the ADQL `query` at the specified TAP service, and return the result as a `DictArray` - a fully featured Julia table.

`kwargs` are passed to `VOTables.read`, for example specify `unitful=true` to parse columns with units into `Unitful.jl` values.
"""
execute(tap::TAPService, query::AbstractString; kwargs...) = @p let
	tap.baseurl
	@modify(joinpath(_, "sync"), __.path)
	URI(__; query = [
		"request" => "doQuery",
		"lang" => "adql",
		"query" => strip(query),
	])
	download
	VOTables.read(; kwargs...)
end

"""    VizierCatalog(id; [unitful=false])

A catalog from the [VizieR](https://vizier.u-strasbg.fr/) database, identified by its `id` (e.g. `"J/ApJS/260/4/table2"`).

Main capabilities:
- Download as a raw file: `download(::VizierCatalog)`
- Retrieve as a Julia table (`DictArray`): `table(::VizierCatalog)`
- Crossmatch using the [CDS X-Match service](https://cdsxmatch.u-strasbg.fr/xmatch): the `FlexiJoins` interface, `innerjoin((; ::VizierCatalog, tbl), by_distance(identity, ..., separation, <=(...)))`

Keyword arguments control accessing or processing the catalog:
- `unitful=false`: whether to parse columns with units into `Unitful.jl` values
- `table_format="votable"`: format of the downloaded table, only supported for downloading the raw file
"""
Base.@kwdef struct VizierCatalog
	id::String
	unitful::Bool = false
	table_format::String = "votable"
	params_string::String = "-out=**&-out.max=unlimited" # -out=_RAJ2000,_DEJ2000,_sed0,**
end

VizierCatalog(id; kwargs...) = VizierCatalog(; id, kwargs...)

function Base.download(c::VizierCatalog, path=tempname())
	# output = IOBuffer()
	# Downloads.request("https://vizier.cds.unistra.fr/viz-bin/votable"; method="POST", input=IOBuffer("""-source=$(c.id)&$(c.params_string)"""), output)
	# seekstart(output)
	# write(path, output)
	download("https://vizier.cds.unistra.fr/viz-bin/votable?-source=$(c.id)&$(c.params_string)", path)
end

table(c::VizierCatalog; kwargs...) = VOTables.read(download(c); c.unitful, kwargs...)

function vizier_xmatch_vot(A, B, maxsep)
	params_A = @p xmatch_catalog_to_form_params(A) |> @modify(k -> "$(k)1", __ |> Elements() |> first)
	params_B = @p xmatch_catalog_to_form_params(B) |> @modify(k -> "$(k)2", __ |> Elements() |> first)
	params = vcat(params_A, params_B)
	params_cmd = @p params |> map("-F$(first(_))=$(last(_))")
	outvot = IOBuffer()
	# XXX: couldn't make the same request with HTTP.jl
	run(pipeline(`
		$(curl()) -X POST
		-F REQUEST=xmatch
		-F distMaxArcsec=$(rad2deg(maxsep) * 60^2)
		-F RESPONSEFORMAT=votable
		$(params_cmd)
		http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync
	`; stdout=outvot))
	seekstart(outvot)
	return outvot
end

xmatch_catalog_to_form_params((c, _)::Tuple{VizierCatalog, typeof(identity)}) = ["cat" => "vizier:$(c.id)"]

function xmatch_catalog_to_form_params((c, coordsf)::Tuple{<:AbstractVector, <:Any})
	tbl = @p let
		c
		StructArray(
			_key=keys(__),
			coords=map(coordsf, __),
		)
		mapinsert(
			# XXX: here we assume coords have "ra" and "dec" properties in radians
			# should somehow put this into SkyCoords extension?..
			_ra_d=rad2deg(_.coords.ra),
			_dec_d=rad2deg(_.coords.dec),
		)
		@delete __.coords
	end
	csv_file = tempname()
	tbl |> CSV.write(csv_file)
	["cat" => "@$csv_file", "colRA" => "_ra_d", "colDec" => "_dec_d"]
end


# XXX: piracy
# see https://github.com/JuliaWeb/URIs.jl/pull/55
Base.download(uri::URI, args...) = download(URIs.uristring(uri), args...)

end
