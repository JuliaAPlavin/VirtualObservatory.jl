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


struct TAPService
	baseurl::URI
	format::String
end
TAPService(baseurl::AbstractString, format="VOTABLE/TD") = TAPService(URI(baseurl), format)

_TAP_SERVICES = Dict(
	:vizier => TAPService("http://tapvizier.cds.unistra.fr/TAPVizieR/tap"),
	:simbad => TAPService("https://simbad.u-strasbg.fr/simbad/sim-tap"),
	:ned => TAPService("https://ned.ipac.caltech.edu/tap"),
	:gaia => TAPService("https://gea.esac.esa.int/tap-server/tap", "VOTABLE_PLAIN"),
)
TAPService(service::Symbol) = _TAP_SERVICES[service]

@doc """
    TAPService(baseurl, [format="VOTABLE/TD"])
    TAPService(service::Symbol)

Handler of a service following the Virtual Observatory Table Access Protocol (TAP), as [defined](https://www.ivoa.net/documents/TAP/) by IVOA.
Instances of `TAPService` can be created by passing either a base URL of the service or a symbol corresponding to a known service:
$(@p keys(_TAP_SERVICES) |> collect |> sort |> map("`:$_`") |> join(__, ", ")).

A `TAPService` aims to follow the `DBInterface` interface, with query execution as the main feature: `execute(tap, query::String)`.
""" TAPService

connect(::Type{TAPService}, args...) = TAPService(args...)

struct TAPTable
	service::TAPService
	tablename::String
	unitful::Bool
	ra_col::String
	dec_col::String
	cols
end
TAPTable(service, tablename, cols=All(); unitful=true, ra_col="ra", dec_col="dec") = TAPTable(service, tablename, unitful, ra_col, dec_col, cols)

"""    execute([restype=StructArray], tap::TAPService, query::AbstractString; kwargs...)

Execute the ADQL `query` at the specified TAP service, and return the result as a `StructArray` - a fully featured Julia table.

`kwargs` are passed to `VOTables.read`, for example specify `unitful=true` to parse columns with units into `Unitful.jl` values.
"""
execute(tap::TAPService, query::AbstractString; upload=nothing, kwargs...) = execute(StructArray, tap, query; upload, kwargs...)
execute(T::Type, tap::TAPService, query::AbstractString; upload=nothing, kwargs...) = @p download(tap, query; upload) |> VOTables.read(T; kwargs...)

function Base.download(tap::TAPService, query::AbstractString, path=tempname(); upload=nothing)
	syncurl = @p tap.baseurl |> @modify(joinpath(_, "sync"), __.path)
	if isnothing(upload)
		# should probably work with POST as well by design, but some services prefer GET when possible
		@p let
			syncurl
			URI(__; query = [
				"request" => "doQuery",
				"lang" => "adql",
				"FORMAT" => tap.format,
				"query" => strip(query),
			])
			download(__, path)
		end
	else
		# XXX: try to make the same request with HTTP.jl
		@p let
			syncurl
			`
			curl -X POST
			-F REQUEST=doQuery
			-F LANG=ADQL
			-F FORMAT="$(tap.format)"
			-F QUERY=$('"' * replace(strip(query), "\"" => "\\\"") * '"')
			$(tap_upload_cmd(upload))
			--insecure
			--output $path
			$(URIs.uristring(__))
			`
			run(pipeline(__))
			@_ path
		end
	end
end

tap_upload_cmd(::Nothing) = []
tap_upload_cmd(upload) = @p let
	upload
	map(keys(__), values(__)) do k, tbl
		vot_file = tempname()
		tbl |> VOTables.write(vot_file)
		["-F UPLOAD=$k,param:$k", "-F $k=@$vot_file"]
	end
	flatten
end

"""    VizierCatalog(id, [cols=All()]; [unitful=false])

A catalog from the [VizieR](https://vizier.u-strasbg.fr/) database, identified by its `id` (e.g. `"J/ApJS/260/4/table2"`).

Main capabilities:
- Download as a raw file: `download(::VizierCatalog)`
- Retrieve as a Julia table (`StructArray`): `table(::VizierCatalog)`
- Crossmatch using the [CDS X-Match service](https://cdsxmatch.u-strasbg.fr/xmatch): the `FlexiJoins` interface, `innerjoin((; ::VizierCatalog, tbl), by_distance(identity, ..., separation, <=(...)))`

Arguments control accessing or processing the catalog:
- `cols=All()`: only retrieve selected columns
- `unitful=false`: whether to parse columns with units into `Unitful.jl` values
- `table_format="votable"`: format of the downloaded table, only supported for downloading the raw file
"""
Base.@kwdef struct VizierCatalog
	id::String
	cols
	unitful::Bool = false
	table_format::String = "votable"
	params_string::String = "-out.max=unlimited" # -out=_RAJ2000,_DEJ2000,_sed0,**
end

VizierCatalog(id, cols=All(); kwargs...) = VizierCatalog(; id, cols, kwargs...)

function Base.download(c::VizierCatalog, path=tempname())
	# output = IOBuffer()
	# Downloads.request("https://vizier.cds.unistra.fr/viz-bin/votable"; method="POST", input=IOBuffer("""-source=$(c.id)&$(c.params_string)"""), output)
	# seekstart(output)
	# write(path, output)
	download("https://vizier.cds.unistra.fr/viz-bin/votable?-source=$(c.id)&-out=$(_colspec_to_urlparam(c.cols))&$(c.params_string)", path)
end

_colspec_to_urlparam(::All) = "**"
_colspec_to_urlparam(cols::Cols{<:Tuple}) = join(cols.cols, ",")
_colspec_to_urlparam(cols::Cols{<:Tuple{Union{Tuple,Vector}}}) = join(only(cols.cols), ",")

_table_type_from_coldef(_) = StructArray

table(c::VizierCatalog; kwargs...) = VOTables.read(_table_type_from_coldef(c.cols), download(c); c.unitful, kwargs...)
StructArrays.StructArray(c::VizierCatalog; kwargs...) = VOTables.read(StructArray, download(c); c.unitful, kwargs...)

StructArrays.StructArray(t::TAPTable) = execute(StructArray, t.service, "select * from \"$(t.tablename)\"")

function vizier_xmatch_vot(A, B, maxsep)
	params_A = @p xmatch_catalog_to_form_params(A) |> @modify(k -> "$(k)1", __ |> Elements() |> first)
	params_B = @p xmatch_catalog_to_form_params(B) |> @modify(k -> "$(k)2", __ |> Elements() |> first)
	params = vcat(params_A, params_B)
	params_cmd = @p params |> map("-F$(first(_))=$(last(_))")
	outvot = tempname()
	# XXX: couldn't make the same request with HTTP.jl
	run(pipeline(`
		$(curl()) -X POST
		-F REQUEST=xmatch
		-F distMaxArcsec=$(rad2deg(maxsep) * 60^2)
		-F RESPONSEFORMAT=votable
		--output $(outvot)
		$(params_cmd)
		http://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync
	`))
	return outvot
end

function xmatch_catalog_to_form_params((c, _)::Tuple{VizierCatalog, typeof(identity)})
	params = ["cat" => "vizier:$(c.id)"]
	if c.cols isa Cols
		push!(params, "cols" => _colspec_to_urlparam(c.cols))
	end
	return params
end

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
