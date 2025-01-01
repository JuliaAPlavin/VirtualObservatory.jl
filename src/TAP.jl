struct TAPService
    baseurl::URI
    format::String
end
TAPService(baseurl::AbstractString; format="VOTABLE/TD") = TAPService(URI(baseurl), format)

_TAP_SERVICES = Dict(
    :vizier => TAPService("http://tapvizier.cds.unistra.fr/TAPVizieR/tap"),
    :simbad => TAPService("https://simbad.u-strasbg.fr/simbad/sim-tap"),
    :ned => TAPService("https://ned.ipac.caltech.edu/tap"),
    :gaia => TAPService("https://gea.esac.esa.int/tap-server/tap", format="VOTABLE_PLAIN"),
    :cadc => TAPService("https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus", format="VOTABLE"),
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

StructArrays.StructArray(t::TAPTable) = execute(StructArray, t.service, "select * from \"$(t.tablename)\"")

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
                "lang" => "ADQL",
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
            curl
            -F REQUEST=doQuery
            -F LANG=ADQL
            -F FORMAT="$(tap.format)"
            -F QUERY=$('"' * replace(strip(query), "\"" => "\\\"") * '"')
            $(tap_upload_cmd(upload))
            --insecure
            --output $path
            --location
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
