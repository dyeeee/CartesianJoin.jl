function my_cartesianjoin_v4(dsl::AbstractDataset, dsr::AbstractDataset;
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

  oncols_left = onleft
  oncols_right = onright

  #println(oncols_left)
  #println(right_cols)

  # get flag, idx and ranges
  #println(flag)
  @timeit "compute flag vector" cross_compare_vec(dsl, dsr, flag, conditions, onleft, onright, threads)
  #cross_compare(dsl,dsr,flag,conditions,onleft,onright,threads)
  #println(flag)

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  @timeit "new ds" nds = generate_newds_v4(flag, dsr, dsr, l_len, r_len, right_cols)

  print_timer()
  flag, nds
end

function cross_compare_vec(dsl, dsr,
  flag, conditions, onleft, onright, threads)

  oncols_left = onleft
  oncols_right = onright

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  for i in 1:length(conditions)  # Each conditions 每个条件
    fun = conditions[i]

    IMD.@_threadsfor threads for j in 1:l_len  # each row in dsl
      cur_index = (j - 1) * l_len
      ## 传递函数的allocations？
      _op_for_dsrcol(flag, fun, cur_index, IMD._columns(dsl)[oncols_left[i]][j], IMD._columns(dsr)[oncols_right[i]], r_len,
        oncols_left, oncols_right)

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


function generate_newds_v4(flag, dsl, dsr, l_len, r_len, right_cols)

  @time begin
    ## new left
    dsl_count = []  # 每一行对应几个右边
    for i in 1:l_len
      lo = 1 + (i - 1) * r_len
      hi = lo + r_len - 1
      push!(dsl_count, count(==(true), flag[lo:hi]))
    end

    new_ends = IMD.our_cumsum!(dsl_count)  # 累计和
    total_length = new_ends[end]

    res = []
    for j in 1:length(IMD.index(dsl))  # left 的每一列
      _res = IMD.allocatecol(IMD._columns(dsl)[j], total_length, addmissing=false)  # 

      x = IMD._columns(dsl)[j] # 这一步可以隔离

      # 左侧填充
      IMD.@_threadsfor true for i in 1:length(x)
        dsl_count[i] == 0 && continue
        i == 1 ? lo = 1 : lo = new_ends[i-1] + 1
        hi = new_ends[i]
        IMD._fill_val_join!(_res, lo:hi, x[i])
      end

      push!(res, _res)
    end

    if dsl isa SubDataset
      newds = Dataset(res, copy(IMD.index(dsl)), copycols=false)
    else
      newds = Dataset(res, IMD.Index(copy(IMD.index(dsl).lookup), copy(IMD.index(dsl).names), copy(IMD.index(dsl).format)), copycols=false)
    end
  end


  @time begin
    ## new right
    #println("Cerating right")

    dsr_idx = []
    for i in 1:l_len
      lo = 1 + (i - 1) * r_len
      hi = lo + r_len - 1
      append!(dsr_idx, findall(isone, flag[lo:hi]))
    end

    #println(dsr_idx)

    #println("Nesting")
    for j in 1:length(right_cols)
      _res = IMD.allocatecol(IMD._columns(dsr)[right_cols[j]], total_length, addmissing=false)  # 空的dsr

      IMD.@_threadsfor true for i in 1:length(l_len)
        #print(j)
        length(dsr_idx[i]) == 0 && continue
        for i in 1:length(dsr_idx)
          _res[i] = IMD._columns(dsr)[right_cols[j]][dsr_idx[i]]
        end
      end

      #println(_res)
      push!(IMD._columns(newds), _res)

      new_var_name = IMD.make_unique([IMD._names(dsl); IMD._names(dsr)[right_cols[j]]], makeunique=true)[end]
      push!(IMD.index(newds), new_var_name)
      setformat!(newds, IMD.index(newds)[new_var_name], getformat(dsr, IMD._names(dsr)[right_cols[j]]))
    end
  end
  newds


end