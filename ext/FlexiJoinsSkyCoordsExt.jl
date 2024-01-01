module FlexiJoinsSkyCoordsExt

using FlexiJoins: ByDistance, swap_sides
import FlexiJoins: innerjoin
using SkyCoords
using VirtualObservatory
using VirtualObservatory: vizier_xmatch_vot
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
		unitful=datas[1].unitful)
	vot_viz = delete(vot_xmatch, @optics _._key _._ra_d _._dec_d _.angDist)
	StructArray(NamedTuple{keys(datas)}((vot_viz, view(datas[2], vot_xmatch._key))))
end

end
