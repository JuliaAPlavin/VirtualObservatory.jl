module FlexiJoinsSkyCoordsExt

using FlexiJoins: ByDistance, swap_sides
import FlexiJoins: innerjoin
using SkyCoords
using VirtualObservatory
using VirtualObservatory: vizier_xmatch_vot, mapinsert
using VirtualObservatory.DataPipes, VirtualObservatory.VOTables, VirtualObservatory.AccessorsExtra, VirtualObservatory.StructArrays


function innerjoin(
        datas::NamedTuple{<:Any, <:Union{Tuple{VizierCatalog, Any}, Tuple{Any, VizierCatalog}}},
        cond::ByDistance{<:Any, <:Any, typeof(separation)}
    )
	viz_ix = @p datas |> Tuple |> findall(_ isa VizierCatalog)
	length(viz_ix) == 1 || error("Joining two VizieR catalogs not supported yet")
	viz_ix = only(viz_ix)
	if viz_ix == 2
		return swap_sides(innerjoin(swap_sides(datas), swap_sides(cond)))
	end
	@assert viz_ix == 1
	
	votfile = vizier_xmatch_vot((datas[1], cond.func_L), (datas[2], cond.func_R), cond.max)
	vot_xmatch = VOTables.read(
		VirtualObservatory._table_type_from_coldef(datas[1].cols),
		votfile;
		datas[1].unitful)
	vot_viz = delete(vot_xmatch, @optics _._key _._ra_d _._dec_d _.angDist)
	StructArray(NamedTuple{keys(datas)}((vot_viz, view(datas[2], vot_xmatch._key))))
end

function innerjoin(
        datas::NamedTuple{<:Any, <:Union{Tuple{TAPTable, Any}, Tuple{Any, TAPTable}}},
        cond::ByDistance{<:Any, <:Any, typeof(separation)}
    )
	viz_ix = @p datas |> Tuple |> findall(_ isa TAPTable)
	length(viz_ix) == 1 || error("Joining two TAP tables not supported yet")
	viz_ix = only(viz_ix)
	if viz_ix == 2
		return swap_sides(innerjoin(swap_sides(datas), swap_sides(cond)))
	end
	@assert viz_ix == 1
	
	restype = VirtualObservatory._table_type_from_coldef(datas[1].cols)

	vot_xmatch = @p let
		datas[2]
		StructArray(
			my_key=keys(__),
			coords=map(cond.func_R, __),
		)
		mapinsert(
			# XXX: here we assume coords have "ra" and "dec" properties in radians
			ra_d=rad2deg(_.coords.ra),
			dec_d=rad2deg(_.coords.dec),
		)
		@delete __.coords
		execute(restype, datas[1].service, """
			SELECT
				my.my_key,
				$(_cols_to_sql(datas[1].cols))
			FROM TAP_UPLOAD.my
			JOIN $(datas[1].tablename) AS taptbl
			ON 1 = CONTAINS(
				POINT(my.ra_d, my.dec_d),
				CIRCLE(taptbl.$(datas[1].ra_col), taptbl.$(datas[1].dec_col), $(rad2deg(cond.max)))
			)
			""";
			upload=(my=__,), datas[1].unitful)
	end
	vot_viz = delete(vot_xmatch, @optics _.my_key)
	StructArray(NamedTuple{keys(datas)}((vot_viz, view(datas[2], vot_xmatch.my_key))))
end

_cols_to_sql(::All) = "taptbl.*"
_cols_to_sql(cols::Cols) = VirtualObservatory._colspec_to_urlparam(cols)

end
