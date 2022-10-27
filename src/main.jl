
# 功能对外入口，解析参数后调用内部函数 _


using InMemoryDatasets
#using Revise

include("join_cartesian.jl")

function _cartesianjoin_test()
  _test()
end

function cartesianjoin_timer(dsl::AbstractDataset, dsr::AbstractDataset;
  on=[], threads::Bool=true,
  makeunique=false, mapformats::Union{Bool,Vector{Bool}}=true,
  check=true,
  multiple_match::Union{Bool,Vector{Bool}}=false, multiple_match_name=:multiple,
  obs_id::Union{Bool,Vector{Bool}}=false, obs_id_name=:obs_id)


  # 解析参数后调用内部函数 _
  if !(on isa AbstractVector)
    on = [on]
  else
    on = on
  end
  if !(mapformats isa AbstractVector)
    mapformats = repeat([mapformats], 2)
  else
    length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
  end
  if !(multiple_match isa AbstractVector)
    multiple_match = repeat([multiple_match], 2)
  else
    length(multiple_match) !== 2 && throw(ArgumentError("`multiple_match` must be a Bool or a vector of Bool with size two"))
  end
  if !(obs_id isa AbstractVector)
    obs_id = repeat([obs_id], 2)
  else
    length(obs_id) !== 2 && throw(ArgumentError("`obs_id` must be a Bool or a vector of Bool with size two"))
  end

  if isempty(on) ## cj without any conditions
    return _join_cartesian_timer(dsl, dsr, [], nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64),
      onleft=[], onright=[], onright_equal=[], threads=threads,
      makeunique=makeunique, mapformats=mapformats, check=check,
      multiple_match=multiple_match, multiple_match_name=multiple_match_name,
      obs_id=obs_id, obs_id_name=obs_id_name)
  end

  if (typeof(on) <: AbstractVector{<:Pair{<:IMD.ColumnIndex,<:Any}})

    dsr_cols = Symbol[]
    equalon_dsr_cols = Symbol[]
    conditions = Function[]

    #(typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}})
    #(typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})

    # on  = [:xid => :yid, :x1 => :y1 => isless]
    for element in map(x -> x.second, on)
      if typeof(element) <: Pair
        # TODO more function check
        !(element.second isa Function) && throw(ArgumentError("Need Function"))
        push!(dsr_cols, element.first)
        push!(conditions, element.second)
      elseif typeof(element) <: IMD.ColumnIndex  # default condition isequal
        push!(dsr_cols, element)
        push!(equalon_dsr_cols, element)
        push!(conditions, isequal)
      else
        throw(ArgumentError("error `on`, e.g. on  = [:xid => :yid, :x1 => :y1 => isless]"))
      end
    end
    onleft = IMD.multiple_getindex(IMD.index(dsl), map(x -> x.first, on))
    onright = IMD.multiple_getindex(IMD.index(dsr), dsr_cols)
    onright_equal = IMD.multiple_getindex(IMD.index(dsr), equalon_dsr_cols)
  else
    throw(ArgumentError("error `on`, e.g. on  = [:xid => :yid, :x1 => :y1 => isless]"))
  end

  return _join_cartesian_timer(dsl, dsr, conditions, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64),
    onleft=onleft, onright=onright, onright_equal=onright_equal, threads=threads,
    makeunique=makeunique, mapformats=mapformats, check=check,
    multiple_match=multiple_match, multiple_match_name=multiple_match_name,
    obs_id=obs_id, obs_id_name=obs_id_name)

end

function cartesianjoin(dsl::AbstractDataset, dsr::AbstractDataset;
  on=[], threads::Bool=true,
  makeunique=false, mapformats::Union{Bool,Vector{Bool}}=true,
  check=true,
  multiple_match::Union{Bool,Vector{Bool}}=false, multiple_match_name=:multiple,
  obs_id::Union{Bool,Vector{Bool}}=false, obs_id_name=:obs_id)


  # 解析参数后调用内部函数 _
  if !(on isa AbstractVector)
    on = [on]
  else
    on = on
  end
  if !(mapformats isa AbstractVector)
    mapformats = repeat([mapformats], 2)
  else
    length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
  end
  if !(multiple_match isa AbstractVector)
    multiple_match = repeat([multiple_match], 2)
  else
    length(multiple_match) !== 2 && throw(ArgumentError("`multiple_match` must be a Bool or a vector of Bool with size two"))
  end
  if !(obs_id isa AbstractVector)
    obs_id = repeat([obs_id], 2)
  else
    length(obs_id) !== 2 && throw(ArgumentError("`obs_id` must be a Bool or a vector of Bool with size two"))
  end

  #(typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}})
  if isempty(on) ## cj without any conditions
    return _join_cartesian(dsl, dsr, [], nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64),
      onleft=[], onright=[], onright_equal=[], threads=threads,
      makeunique=makeunique, mapformats=mapformats, check=check,
      multiple_match=multiple_match, multiple_match_name=multiple_match_name,
      obs_id=obs_id, obs_id_name=obs_id_name)
  end

  if (typeof(on) <: AbstractVector{<:Pair{<:IMD.ColumnIndex,<:Any}})

    dsr_cols = []
    equalon_dsr_cols = []
    conditions = Function[]

    # on  = [:xid => :yid, :x1 => :y1 => isless]
    for element in map(x -> x.second, on)
      if typeof(element) <: Pair
        # TODO more function check
        !(element.second isa Function) && throw(ArgumentError("Need Function"))
        push!(dsr_cols, element.first)
        push!(conditions, element.second)
      elseif typeof(element) <: IMD.ColumnIndex  # default condition isequal
        push!(dsr_cols, element)
        push!(equalon_dsr_cols, element)
        push!(conditions, isequal)
      else
        throw(ArgumentError("error `on`. \n e.g.1 on  = [1 => 1, 2 => 2 => isless] (index for right and left columns)\n e.g.2 on  = [:xid => :yid, :x1 => :y1 => isless]\n e.g.3 on  = [\"xid\" => \"yid\", \"x1\" => \"y1\" => isless]"))
      end
    end
    onleft = IMD.multiple_getindex(IMD.index(dsl), map(x -> x.first, on))
    onright = IMD.multiple_getindex(IMD.index(dsr), dsr_cols)
    onright_equal = IMD.multiple_getindex(IMD.index(dsr), equalon_dsr_cols)
  else
    throw(ArgumentError("error `on`. \n e.g.1 on  = [1 => 1, 2 => 2 => isless] (index for right and left columns)\n e.g.2 on  = [:xid => :yid, :x1 => :y1 => isless]\n e.g.3 on  = [\"xid\" => \"yid\", \"x1\" => \"y1\" => isless]"))
  end

  return _join_cartesian(dsl, dsr, conditions, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64),
    onleft=onleft, onright=onright, onright_equal=onright_equal, threads=threads,
    makeunique=makeunique, mapformats=mapformats, check=check,
    multiple_match=multiple_match, multiple_match_name=multiple_match_name,
    obs_id=obs_id, obs_id_name=obs_id_name)

end