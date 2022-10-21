#包的一级入口

module CartesianJoin

using InMemoryDatasets


include("main.jl")


function __init__()
  @info "CartesianJoin.jl is a join extension package developed based on the high-performance data processing package InMemoryDatasets.jl, which expands the `cartesianjoin` function of the `Dataset` object."
end


export cartesianjoin
end
