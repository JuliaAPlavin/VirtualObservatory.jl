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
