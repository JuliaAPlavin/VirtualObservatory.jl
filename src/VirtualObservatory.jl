module VirtualObservatory

using VOTables
import Tables: table
using CURL_jll
using DataPipes
using AccessorsExtra
using FlexiMaps
using StructArrays
using CSV

export VizierCatalog, table


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

end
