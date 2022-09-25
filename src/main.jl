
# 功能对外入口，解析参数后调用内部函数 _
include("my_cartesianjoin_v5.jl")

using InMemoryDatasets


function cartesianjoin(dsl::AbstractDataset)
  dsl
end
"""
function cartesianjoin(dsl::AbstractDataset,
  dsr::AbstractDataset; on=nothing, makeunique=false,
  mapformats::Union{Bool,Vector{Bool}}=true,
  stable=false, alg=HeapSort, check=true, accelerate=false,
  method::Symbol=:sort, threads::Bool=true,
  multiple_match::Bool=false, multiple_match_name=:multiple,
  obs_id::Union{Bool,Vector{Bool}}=false, obs_id_name=:obs_id)


  _test()




end

"""
function _cartesianjoin_test()


  _test()




end