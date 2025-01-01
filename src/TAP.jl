struct TAPService
    baseurl::URI
    format::Union{String,Nothing}
end
TAPService(baseurl::AbstractString; format=nothing) = TAPService(URI(baseurl), format)

_TAP_SERVICES = Dict(
    :vizier => TAPService("http://tapvizier.cds.unistra.fr/TAPVizieR/tap"),
    :simbad => TAPService("https://simbad.u-strasbg.fr/simbad/sim-tap"),
    :ned => TAPService("https://ned.ipac.caltech.edu/tap"),
    :gaia => TAPService("https://gea.esac.esa.int/tap-server/tap"),
    :cadc => TAPService("https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"),
)
function TAPService(service::Symbol; kwargs...)
    svc = _TAP_SERVICES[service]
    setproperties(svc; kwargs...)
end

@doc """
    TAPService(baseurl, [format="VOTABLE/TD"])
    TAPService(service::Symbol)

Handler of a service following the Virtual Observatory Table Access Protocol (TAP), as [defined](https://www.ivoa.net/documents/TAP/) by IVOA.
Instances of `TAPService` can be created by passing either a base URL of the service or a symbol corresponding to a known service:
$(@p keys(_TAP_SERVICES) |> collect |> sort |> map("`:$_`") |> join(__, ", ")).

A `TAPService` aims to follow the `DBInterface` interface, with query execution as the main feature: `execute(tap, query::String)`.

`TAPService` queries use `HTTP.request`. Keyword arguments for `HTTP.request` may be given in the `VirtualObservator.HTTP_REQUEST_OPTIONS` `Dict`.
See the docstrings for `HTTP_REQUEST_OPTIONS` and `HTTP.request` for more details.
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
function execute(T::Type, tap::TAPService, query::AbstractString; upload=nothing, cache=false, read_cache=cache, write_cache=cache, kwargs...)
    file = download(tap, query; upload, cache, read_cache, write_cache)
    tbl = VOTables.read(T, file; kwargs...)
    rm(file)
    return tbl
end

function Base.download(tap::TAPService, query::AbstractString, path=tempname(); upload=nothing, cache=false, read_cache=cache, write_cache=cache)
    if read_cache || write_cache
        if !isnothing(upload)
            error("Caching queries with uploads is not supported")
            # @warn "Caching queries with uploads relies on the uploaded data serialization. Please make sure it remains consistent."
        end
        cache_dct = @p let
            joinpath(@get_scratch!("cache"), "cache.db")
            SQLite.DB(__)
            SQLDictionary{
                @NamedTuple{service_url::String, query_stripped::String},
                @NamedTuple{query::String, response::String}}(
                __, :TAP_downloads)
        end
        query_stripped = @p query strip() replace(__, r"\s+" => " ")
        cache_key = (; service_url=string(tap.baseurl), query_stripped)
        if read_cache && haskey(cache_dct, cache_key)
            (;response) = cache_dct[cache_key]
            open(path, "w") do f
                write(f, response)
            end
            return path
        end
    end

    # URL is the same regardless of uploading or not
    sync_url = @p tap.baseurl |> @modify(joinpath(_, "sync"), __.path)

    # HTTP method, body, query are different for uploading vs not uploading
    if isnothing(upload)
        # not uploading
        method = "GET"
        body = []
        http_query = Pair{String,Any}[[
            "request" => "doQuery",
            "lang" => "ADQL",
            "query" => strip(query),];
            isnothing(tap.format) ? [] : ["FORMAT" => tap.format];
        ]
    else
        # uploading
        method = "POST"
        http_query = []
        formdata = Pair{String,Any}[[
            "request" => "doQuery",
            "lang" => "ADQL",
            "query" => strip(query),];
            isnothing(tap.format) ? [] : ["FORMAT" => tap.format];
            @p pairs(upload) collect flatmap() do (k, tbl)
                    vot_io = IOBuffer()
                    VOTables.write(vot_io, tbl)
                    seekstart(vot_io)

                    ["UPLOAD" => "$k,param:$k",
                        string(k) => HTTP.Multipart("tbl.vot", vot_io, "application/x-votable+xml")]
            end;
        ]
        body = HTTP.Form(formdata)
    end

    # Now make request and write response body to path
    open(path, "w") do response_stream
        # 1. Use require_ssl_verification=false to match the previous use of
        #    curl's --insecure option (even though tests pass without it).
        #
        # 2. Use pool=HTTP.Pool(1) to make tests pass (avoids concurrency
        #    issues?)
        #
        # The user can override these and/or use other HTTP.request kwargs by
        # putting them in the HTTP_REQUEST_OPTIONS Dict{Symbol,Any}.
        headers = []
        resp = HTTP.request(method, sync_url, headers, body;
            require_ssl_verification=false, # Allow user to override these...
            pool=HTTP.Pool(1),
            status_exception=false,
            HTTP_REQUEST_OPTIONS..., # ...with kwargs given here...
            query=http_query, response_stream # ...but not these
        )
        if HTTP.iserror(resp)
            e = HTTP.StatusError(resp.status, method, string(sync_url), resp)
            @error "HTTP request failed" e
        end
    end

    if write_cache
        insert!(cache_dct, cache_key, (;query, response=read(path, String)))
    end

    return path
end
