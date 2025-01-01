struct DataLinkService
    service_id::String
    capability::String
    registry_url::URI
    datalink_url::URI
end

DataLinkService(service_id, capability, registry_url) =
    DataLinkService(service_id, capability, registry_url, find_capability_url(service_id, capability, registry_url))

_DATALINK_SERVICES_ARGS = Dict(
    :cadc => ("ivo://cadc.nrc.ca/caom2ops", "ivo://ivoa.net/std/DataLink#links-1.0", URI("http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/reg/resource-caps")),
)
DataLinkService(service::Symbol) = DataLinkService(_DATALINK_SERVICES_ARGS[service]...)


datalink_table(dl::DataLinkService, row; kwargs...) = @p let
    URI(dl.datalink_url, query="ID" => row.publisherID)
    download()
    VOTables.read(; kwargs...)
end
    

function find_capability_url(service_uri, capability, registry_url)
    caps_url = @p let
        download(registry_url)
        readlines()
        filter(!isempty(_) && !startswith(_, '#'))
        map(__ -> split(__, '=') |> map(strip) |> Pair(first(__), last(__)))
        Dict
        __[string(service_uri)]
        String
    end

    caps_file = download(caps_url)
    final_url = @p let
        VOTables.StringView(VOTables.mmap(caps_file))
        VOTables.parsexml
        VOTables.root
        @aside ns = VOTables._namespaces(__)
        VOTables._findall("ns:capability[@standardID='$capability']", __, ns)
        only
        VOTables._findall("ns:interface/accessURL", __, ns)
        only
        VOTables.nodecontent
    end

    return URI(final_url)
end
