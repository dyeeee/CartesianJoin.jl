
module cartesianjoin

macro _threadsfor(threads, exp)
  esc(:(
    if $threads
      Threads.@threads $exp
    else
      $exp
    end
  ))
end

using InMemoryDatasets
import InMemoryDatasets as IMD
using SparseArrays

function my_cartesianjoin_v3(dsl::AbstractDataset, dsr::AbstractDataset;
  on=nothing, threads::Bool=true, flag=ones(Bool, nrow(dsl), nrow(dsr)))

  dsr_cols = Symbol[]
  equalon_dsr_cols = Symbol[]
  conditions = Function[]

  for element in map(x -> x.second, on)
    if typeof(element) <: Pair
      push!(dsr_cols, element.first)
      push!(conditions, element.second)
    else
      push!(dsr_cols, element)
      push!(equalon_dsr_cols, element)
      push!(conditions, isequal)
    end
  end

  if Set(conditions) == Set([isequal])
    return IMD.innerjoin(dsl, dsr, on=on)
  end

  onleft = IMD.multiple_getindex(IMD.index(dsl), map(x -> x.first, on))
  onright = IMD.multiple_getindex(IMD.index(dsr), dsr_cols)

  equalon_dsr_cols = IMD.multiple_getindex(IMD.index(dsr), equalon_dsr_cols)
  right_cols = setdiff(1:length(IMD.index(dsr)), equalon_dsr_cols)

  oncols_left = onleft
  oncols_right = onright

  # get flag, idx and ranges
  cross_compare_2(dsl, dsr, flag, conditions, onleft, onright, threads)

  ## return flag
  idx, ranges = handle_flag_2(dsl, dsr, sparse(flag), threads)
  # idx, ranges = handle_flag_t(dsl,dsr,sparse(flag),threads)

  # create result
  newds = generate_newds(idx, ranges, dsl, dsr, right_cols, threads)

  newds #,idx,ranges
end

function hello()
  println("Imported.")
end



Base.@propagate_inbounds function _op_for_vector_2(value, vector, fun)
  res = Array{Bool}(undef, length(vector))
  for i in 1:length(vector)
    res[i] = fun(value, vector[i])
  end
  res
end

function cross_compare_2(dsl, dsr,
  flag, conditions, onleft, onright, threads)
  oncols_left = onleft
  oncols_right = onright

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  for i in 1:length(conditions)  # Each conditions 每个条件
    fun = conditions[i]
    @_threadsfor threads for j in 1:l_len # multithread safe? # 可以多线程 逐行判断
      #println(_op_for_vector((IMD._columns(dsl)[oncols_left[i]][j]), IMD._columns(dsr)[oncols_right[1]], fun))
      update_row!(view(flag, j, :), _op_for_vector_2((IMD._columns(dsl)[oncols_left[i]][j]), IMD._columns(dsr)[oncols_right[i]], fun))
    end
  end

  #println(flag)
  flag
end

function update_row!(row, res)
  row .*= res
end


function handle_flag_2(dsl, dsr, flag, threads::Bool=true)
  # 没有聚合ranges 但是空间换时间

  onesres = findall(isone, transpose(flag))
  idx = getindex.(onesres, 1)

  ranges = Vector{UnitRange{Int64}}(undef, nrow(dsl))
  fill!(ranges, 1:0)

  A = getindex.(onesres, 2)
  B = hcat([[i, count(==(i), A)] for i in unique(A)]...)[2, :]  # count each idx
  C = cumsum(B)
  D = [1; C .+ 1]
  # pop!(D);


  loc = 1
  @_threadsfor false for i in unique(A)   # i and loc mismatched when multithread
    ranges[i] = D[loc]:C[loc]
    loc += 1
  end

  idx, ranges
end

function generate_newds(idx, ranges, dsl, dsr, right_cols, threads::Bool=false)
  new_ends = map(length, ranges)
  IMD.our_cumsum!(new_ends)
  total_length = new_ends[end]

  inbits = nothing
  revised_ends = nothing
  #threads = true
  makeunique = false

  res = []
  for j in 1:length(IMD.index(dsl))
    _res = IMD.allocatecol(IMD._columns(dsl)[j], total_length, addmissing=false)
    if IMD.DataAPI.refpool(_res) !== nothing
      IMD._fill_oncols_left_table_inner!(_res.refs, IMD.DataAPI.refarray(IMD._columns(dsl)[j]), ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
    else
      IMD._fill_oncols_left_table_inner!(_res, IMD._columns(dsl)[j], ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=false)
    end
    push!(res, _res)
  end

  if dsl isa SubDataset
    newds = Dataset(res, copy(IMD.index(dsl)), copycols=false)
  else
    newds = Dataset(res, IMD.Index(copy(IMD.index(dsl).lookup), copy(IMD.index(dsl).names), copy(IMD.index(dsl).format)), copycols=false)
  end

  for j in 1:length(right_cols)
    _res = IMD.allocatecol(IMD._columns(dsr)[right_cols[j]], total_length, addmissing=false)
    if IMD.DataAPI.refpool(_res) !== nothing
      IMD._fill_right_cols_table_inner!(_res.refs, view(IMD.DataAPI.refarray(IMD._columns(dsr)[right_cols[j]]), idx), ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
    else
      IMD._fill_right_cols_table_inner!(_res, view(IMD._columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
    end
    push!(IMD._columns(newds), _res)

    new_var_name = IMD.make_unique([IMD._names(dsl); IMD._names(dsr)[right_cols[j]]], makeunique=makeunique)[end]
    push!(IMD.index(newds), new_var_name)
    setformat!(newds, IMD.index(newds)[new_var_name], getformat(dsr, IMD._names(dsr)[right_cols[j]]))
  end


  newds
end

export my_cartesianjoin_v3, hello
end