using TestItems
using TestItemRunner
@run_package_tests


@testitem "_" begin
    import Aqua
    Aqua.test_all(VirtualObservatory; ambiguities=false)
    Aqua.test_ambiguities(VirtualObservatory)

    import CompatHelperLocal as CHL
    CHL.@check()
end
