

function my_cartesianjoin_v5(dsl::AbstractDataset, dsr::AbstractDataset;
  on=nothing, threads::Bool=true, flag=ones(Bool, nrow(dsl) * nrow(dsr)))

  reset_timer!()

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

  onleft = IMD.multiple_getindex(IMD.index(dsl), map(x -> x.first, on))
  onright = IMD.multiple_getindex(IMD.index(dsr), dsr_cols)

  equalon_dsr_cols = IMD.multiple_getindex(IMD.index(dsr), equalon_dsr_cols)
  right_cols = setdiff(1:length(IMD.index(dsr)), equalon_dsr_cols)

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  #println(oncols_left)
  #println(right_cols)

  # get flag
  @timeit "compute flag vector" cross_compare_vec(dsl, dsr, flag, conditions, onleft, onright, l_len, r_len, threads)
  #cross_compare(dsl,dsr,flag,conditions,onleft,onright,threads)
  #println(flag)

  @timeit "new ds" nds = generate_newds_v5(flag, dsl, dsr, l_len, r_len, right_cols)

  print_timer()
  flag, nds
end

function cross_compare_vec(dsl, dsr,
  flag, conditions, onleft, onright, l_len, r_len, threads)

  oncols_left = onleft
  oncols_right = onright

  for i in eachindex(conditions)  #1:length(conditions)  # Each conditions 每个条件
    fun = conditions[i]

    IMD.@_threadsfor threads for j in 1:l_len  # each row in dsl
      cur_index = (j - 1) * l_len
      _op_for_dsrcol(flag, fun, cur_index, IMD._columns(dsl)[onleft[i]][j], IMD._columns(dsr)[oncols_right=onright
          [i]], r_len, onleft, onright)
    end

  end

end

function _op_for_dsrcol(flag, fun, cur_index, x, r_col, r_len, oncols_left, oncols_right)
  for k in 1:r_len
    flag[cur_index+k] == 0 && continue
    #println(IMD._columns(dsl)[oncols_left[i]][j],",",cur_index,",",IMD._columns(dsr)[oncols_right[i]][k])
    flag[cur_index+k] &= fun(x, r_col[k])
  end
end

function generate_newds_v5(flag, dsl, dsr, l_len, r_len, right_cols, threads=true)
  T = Int32

  ## new left
  ### step-1
  dsl_count = Vector{T}(undef, nrow(dsl))  # left每一行对应几个右边

  find_count_for_left(flag, dsl_count, l_len, r_len)

  new_ends = cumsum(dsl_count)  # 累计和
  total_length = new_ends[end]

  ### step-2
  res = []  # 固定尺寸不用push？
  for j in 1:length(IMD.index(dsl))  # left 的每一列
    _res = IMD.allocatecol(IMD._columns(dsl)[j], total_length, addmissing=false)  # 

    fill_left_res(_res, IMD._columns(dsl)[j], dsl_count, new_ends, threads)

    push!(res, _res)
  end

  ### step-3
  if dsl isa SubDataset
    newds = Dataset(res, copy(IMD.index(dsl)), copycols=false)
  else
    newds = Dataset(res, IMD.Index(copy(IMD.index(dsl).lookup), copy(IMD.index(dsl).names), copy(IMD.index(dsl).format)), copycols=false)
  end

  #println(newds)

  ## new right
  #println("Cerating right")
  @timeit "findall" begin
    ### step-4
    dsr_idx = findall(isone, flag)  # 全局索引，要去做出局部索引
    for i in 1:l_len
      dsl_count[i] == 0 && continue

      i == 1 ? lo = 1 : lo = new_ends[i-1] + 1
      hi = new_ends[i]

      dsr_idx[lo:hi] .-= (i - 1) * r_len
      #for k in lo:hi
      #    all[k] -= (i-1)*r_len
      #end
    end
  end

  @timeit "newds left" begin
    ### step-5
    #println("Nesting")
    for j in eachindex(right_cols) #1:length(right_cols)   # right 的每一列
      _res = IMD.allocatecol(IMD._columns(dsr)[right_cols[j]], total_length, addmissing=false)  # 空的dsr

      fill_right_res(_res, IMD._columns(dsr)[right_cols[j]], dsr_idx, threads)

      #println(_res)
      push!(IMD._columns(newds), _res)
      new_var_name = IMD.make_unique([IMD._names(dsl); IMD._names(dsr)[right_cols[j]]], makeunique=true)[end]
      push!(IMD.index(newds), new_var_name)
      setformat!(newds, IMD.index(newds)[new_var_name], getformat(dsr, IMD._names(dsr)[right_cols[j]]))
    end
  end

  newds
end

function find_count_for_left(flag, dsl_count, l_len, r_len)
  for i in 1:l_len
    lo = 1 + (i - 1) * r_len
    hi = lo + r_len - 1
    #push!(dsl_count,count(==(true),flag[lo:hi]))
    @inbounds dsl_count[i] = count(view(flag, lo:hi))
  end
end

function fill_left_res(_res, l_col, dsl_count, new_ends, threads)
  # x = IMD._columns(dsl)[j] # 这一步可以隔离
  # 左侧填充
  IMD.@_threadsfor threads for i in eachindex(l_col)# 1:length(l_col)
    dsl_count[i] == 0 && continue
    i == 1 ? lo = 1 : lo = new_ends[i-1] + 1
    hi = new_ends[i]
    IMD._fill_val_join!(_res, lo:hi, l_col[i])
  end
end

function fill_right_res(_res, r_col, dsr_idx, threads)
  IMD.@_threadsfor threads for i in eachindex(dsr_idx)#1:length(dsr_idx) # 1:length(l_len)  # left 的每一列
    _res[i] = r_col[dsr_idx[i]]
  end
end

