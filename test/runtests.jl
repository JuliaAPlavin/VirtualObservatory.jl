using TestItems
using TestItemRunner
@run_package_tests


@testitem "vizier catalog" begin
    using Dates
    using Unitful

    c = table(VizierCatalog("J/ApJ/923/67/table2"; unitful=false))
    @test c.recno == 1:7728
    @test c[1].ID == "0003+380"
    @test c.var"nu-obs"[1] == 15.37f0
    @test c[2].Epoch == Date(2006, 12, 1)

    c = table(VizierCatalog("J/ApJ/923/67/table2"; unitful=true))
    @test c.recno == 1:7728
    @test c[1].ID == "0003+380"
    @test c.var"nu-obs"[1] == 15.37f0u"GHz"
    @test c[2].Epoch == Date(2006, 12, 1)
end

@testitem "join" begin
    using FlexiJoins
    using SkyCoords
    using Unitful

    tbl = [
        (name="Abc", coords=ICRSCoords(0, 0)),
        (name="Def", coords=ICRSCoords(0.5, -0.1)),
    ]

    c = VizierCatalog("I/355/gaiadr3"; unitful=false)
    J = innerjoin((; c, tbl), by_distance(identity, :coords, separation, <=(deg2rad(1/60))))
    @test length(J) == 5
    @test J[1].c.DR3Name == "Gaia DR3 2546034966433885568"
    @test J[1].c.RAdeg === 0.00943691398
    @test J.tbl[1] === tbl[1]

    c = VizierCatalog("I/355/gaiadr3"; unitful=true)
    J = innerjoin((; c, tbl), by_distance(identity, :coords, separation, <=(deg2rad(1/60))))
    @test length(J) == 5
    @test J[1].c.DR3Name == "Gaia DR3 2546034966433885568"
    @test J[1].c.RAdeg === 0.00943691398u"°"
    @test J.tbl[1] === tbl[1]
end


@testitem "_" begin
    import Aqua
    Aqua.test_all(VirtualObservatory; ambiguities=false)
    Aqua.test_ambiguities(VirtualObservatory)

    import CompatHelperLocal as CHL
    CHL.@check()
end
