
## INNER CALL
import InMemoryDatasets as IMD
using TimerOutputs
include("functions.jl")



function _join_cartesian(dsl::AbstractDataset, dsr::AbstractDataset, conditions, ::Val{T};
  onleft, onright, onright_equal,
  threads::Bool=false, flag=ones(Bool, nrow(dsl) * nrow(dsr)),
  makeunique=false, mapformats=[true, true], check=true,
  multiple_match=[false, false], multiple_match_name=:multiple,
  obs_id=[false, false], obs_id_name=:obs_id) where {T}

  (isempty(dsl) || isempty(dsr)) && throw(ArgumentError("in `cartesianjoin` both left and right tables must be non-empty"))

  onleft = onleft
  onright = onright
  right_cols = setdiff(1:length(IMD.index(dsr)), onright_equal)

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  # cj with conditions
  if !isempty(onleft) && !isempty(onright)
    # get flag
    @timeit "compute flag vector" _cross_compare_vec(dsl, dsr, flag, conditions, onleft, onright, l_len, r_len, threads, mapformats)
  end

  ## new left
  ### step-1
  dsl_count = Vector{T}(undef, nrow(dsl))  # left每一行对应几个右边

  _find_count_for_left(flag, dsl_count, l_len, r_len)

  new_ends = cumsum(dsl_count)  # 
  total_length = new_ends[end]

  if check
    @assert total_length < 10 * nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
  end

  ### step-2
  res = []  # 固定尺寸不用push？
  for j in 1:length(IMD.index(dsl))  # left 的每一列
    _res = IMD.allocatecol(IMD._columns(dsl)[j], total_length, addmissing=false)  # 

    _fill_left_res(_res, IMD._columns(dsl)[j], dsl_count, new_ends, threads)

    push!(res, _res)
  end

  ### step-3
  if dsl isa SubDataset
    newds = Dataset(res, copy(IMD.index(dsl)), copycols=false)
  else
    newds = Dataset(res, IMD.Index(copy(IMD.index(dsl).lookup), copy(IMD.index(dsl).names), copy(IMD.index(dsl).format)), copycols=false)
  end

  ## new right
  #println("Cerating right")

  ### step-5
  #println("Nesting")
  for j in eachindex(right_cols) #1:length(right_cols)   # right 的每一列

    if !isempty(onright)
      _res = IMD.allocatecol(IMD._columns(dsr)[right_cols[j]], total_length, addmissing=false)  # 空的dsr
      _fill_right_res(_res, IMD._columns(dsr)[right_cols[j]], flag, r_len, threads)
    else
      _res = _get_right_res_all(IMD._columns(dsr)[right_cols[j]], l_len)
    end

    #println(_res)
    push!(IMD._columns(newds), _res)
    new_var_name = IMD.make_unique([IMD._names(dsl); IMD._names(dsr)[right_cols[j]]], makeunique=makeunique)[end]
    push!(IMD.index(newds), new_var_name)
    setformat!(newds, IMD.index(newds)[new_var_name], getformat(dsr, IMD._names(dsr)[right_cols[j]]))
  end


  if multiple_match[1]
    multiple_match_name1 = Symbol(multiple_match_name, "_left")
    multiple_match_col_left = _create_multiple_match_col_cartesian(dsl_count, nothing, total_length)
    insertcols!(newds, ncol(newds) + 1, multiple_match_name1 => multiple_match_col_left, unsupported_copy_cols=false, makeunique=makeunique)
  end


  if multiple_match[2]
    dsr_idx = _get_dsr_idx(flag, dsl_count, l_len, r_len, new_ends)
    multiple_match_name2 = Symbol(multiple_match_name, "_right")
    multiple_match_col_right = map(x -> count(==(x), dsr_idx) > 1 ? 1 : 0, dsr_idx)
    insertcols!(newds, ncol(newds) + 1, multiple_match_name2 => multiple_match_col_right, unsupported_copy_cols=false, makeunique=makeunique)
  end


  if obs_id[1]
    obs_id_name1 = Symbol(obs_id_name, "_left")
    obs_id_left = IMD.allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
    # _fill_oncols_left_table_inner!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
    _fill_left_res(obs_id_left, 1:nrow(dsl), dsl_count, new_ends, true)
    insertcols!(newds, ncol(newds) + 1, obs_id_name1 => obs_id_left, unsupported_copy_cols=false, makeunique=makeunique)
  end

  if obs_id[2]
    dsr_idx = _get_dsr_idx(flag, dsl_count, l_len, r_len, new_ends)
    obs_id_name2 = Symbol(obs_id_name, "_right")
    obs_id_right = dsr_idx#allocatecol(T, total_length)
    #_fill_right_cols_table_inner!(obs_id_right, idx, ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
    insertcols!(newds, ncol(newds) + 1, obs_id_name2 => obs_id_right, unsupported_copy_cols=false, makeunique=makeunique)
  end

  newds
end


function _join_cartesian_timer(dsl::AbstractDataset, dsr::AbstractDataset, conditions, ::Val{T};
  onleft, onright, onright_equal,
  threads::Bool=false, flag=ones(Bool, nrow(dsl) * nrow(dsr)),
  makeunique=false, mapformats=[true, true], check=true,
  multiple_match=[false, false], multiple_match_name=:multiple,
  obs_id=[false, false], obs_id_name=:obs_id) where {T}

  reset_timer!()

  onleft = onleft
  onright = onright
  right_cols = setdiff(1:length(IMD.index(dsr)), onright_equal)

  l_len = nrow(dsl)
  r_len = nrow(dsr)

  # cj with conditions
  if !isempty(onleft) && !isempty(onright)
    # get flag
    @timeit "compute flag vector" _cross_compare_vec(dsl, dsr, flag, conditions, onleft, onright, l_len, r_len, threads, mapformats)
  end


  @timeit "generate new ds" begin

    ## new left
    ### step-1
    @timeit "2-1 left row count" begin
      dsl_count = Vector{T}(undef, nrow(dsl))  # left每一行对应几个右边

      _find_count_for_left(flag, dsl_count, l_len, r_len)

      new_ends = cumsum(dsl_count)  # 累计和
      total_length = new_ends[end]
    end

    if check
      @assert total_length < 10 * nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    ### step-2
    @timeit "2-2 left row _res" begin
      res = []  # 固定尺寸不用push？
      for j in 1:length(IMD.index(dsl))  # left 的每一列
        @timeit "2.1 init _res" _res = IMD.allocatecol(IMD._columns(dsl)[j], total_length, addmissing=false)  # 

        @timeit "2.2 fill _res" _fill_left_res(_res, IMD._columns(dsl)[j], dsl_count, new_ends, threads)

        @timeit "2.3 push _res" push!(res, _res)
      end
    end

    ### step-3
    @timeit "2-3 newds dsl" begin
      if dsl isa SubDataset
        newds = Dataset(res, copy(IMD.index(dsl)), copycols=false)
      else
        newds = Dataset(res, IMD.Index(copy(IMD.index(dsl).lookup), copy(IMD.index(dsl).names), copy(IMD.index(dsl).format)), copycols=false)
      end
    end

    ## new right
    #println("Cerating right")

    @timeit "2-5 newds dsr" begin
      ### step-5
      #println("Nesting")
      for j in eachindex(right_cols) #1:length(right_cols)   # right 的每一列

        if !isempty(onright)
          @timeit "5.1 init _res" _res = IMD.allocatecol(IMD._columns(dsr)[right_cols[j]], total_length, addmissing=false)  # 空的dsr

          #@timeit "5.2 fill _res" fill_right_res(_res, IMD._columns(dsr)[right_cols[j]], dsr_idx, threads)

          @timeit "5.2 fill _res" _fill_right_res(_res, IMD._columns(dsr)[right_cols[j]], flag, r_len, threads)
          # @timeit "5.3 fill _res" _fill_right_res_2(_res, IMD._columns(dsr)[right_cols[j]], flag, r_len, threads)
          # println(_res)
        else
          @timeit "5.1 get _res for all CJ" _res = _get_right_res_all(IMD._columns(dsr)[right_cols[j]], l_len)
        end


        push!(IMD._columns(newds), _res)
        new_var_name = IMD.make_unique([IMD._names(dsl); IMD._names(dsr)[right_cols[j]]], makeunique=makeunique)[end]
        push!(IMD.index(newds), new_var_name)
        setformat!(newds, IMD.index(newds)[new_var_name], getformat(dsr, IMD._names(dsr)[right_cols[j]]))
      end
    end


    if multiple_match[1]
      multiple_match_name1 = Symbol(multiple_match_name, "_left")
      multiple_match_col_left = _create_multiple_match_col_cartesian(dsl_count, nothing, total_length)
      insertcols!(newds, ncol(newds) + 1, multiple_match_name1 => multiple_match_col_left, unsupported_copy_cols=false, makeunique=makeunique)
    end


    if multiple_match[2]
      dsr_idx = _get_dsr_idx(flag, dsl_count, l_len, r_len, new_ends)
      multiple_match_name2 = Symbol(multiple_match_name, "_right")
      multiple_match_col_right = map(x -> count(==(x), dsr_idx) > 1 ? 1 : 0, dsr_idx)
      insertcols!(newds, ncol(newds) + 1, multiple_match_name2 => multiple_match_col_right, unsupported_copy_cols=false, makeunique=makeunique)
    end


    if obs_id[1]
      obs_id_name1 = Symbol(obs_id_name, "_left")
      obs_id_left = IMD.allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
      # _fill_oncols_left_table_inner!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
      _fill_left_res(obs_id_left, 1:nrow(dsl), dsl_count, new_ends, true)
      insertcols!(newds, ncol(newds) + 1, obs_id_name1 => obs_id_left, unsupported_copy_cols=false, makeunique=makeunique)
    end

    if obs_id[2]
      dsr_idx = _get_dsr_idx(flag, dsl_count, l_len, r_len, new_ends)
      obs_id_name2 = Symbol(obs_id_name, "_right")
      obs_id_right = dsr_idx#allocatecol(T, total_length)
      #_fill_right_cols_table_inner!(obs_id_right, idx, ranges, new_ends, total_length; inbits=inbits, en2=revised_ends, threads=threads)
      insertcols!(newds, ncol(newds) + 1, obs_id_name2 => obs_id_right, unsupported_copy_cols=false, makeunique=makeunique)
    end


    #newds #, dsl_count, total_length
  end

  print_timer()
  #flag
  newds
end

function _cross_compare_vec(dsl, dsr,
  flag, conditions, onleft, onright, l_len, r_len, threads, mapformats)

  onleft = onleft
  onright = onright

  for i in eachindex(conditions)  #1:length(conditions)  # Each conditions 每个条件
    fun = conditions[i]

    var_l = IMD._columns(dsl)[onleft[i]]
    var_r = IMD._columns(dsr)[onright[i]]
    l_idx = onleft[i]
    r_idx = onright[i]
    if mapformats[1]
      _fl = getformat(dsl, l_idx)
      val_l_cpy = map(_fl, var_l)
    else
      _fl = identity
      val_l_cpy = var_l
    end
    if mapformats[2]
      _fr = getformat(dsr, r_idx)
      var_r_cpy = map(_fr, var_r)
    else
      _fr = identity
      var_r_cpy = var_r
    end


    _left_compare(flag, val_l_cpy, var_r_cpy, l_len, r_len, fun, threads)
  end

end


function _left_compare(flag, l_col, r_col, l_len, r_len, fun, threads)
  IMD.@_threadsfor threads for j in 1:l_len  # each row in dsl
    cur_index = (j - 1) * r_len  # cross join left row to right dataset
    _op_for_dsrcol(flag, fun, cur_index, l_col[j], r_col, r_len)
  end
end


function _op_for_dsrcol(flag, fun, cur_index, x, r_col, r_len)
  for k in 1:r_len
    flag[cur_index+k] == 0 && continue
    #println(IMD._columns(dsl)[oncols_left[i]][j],",",cur_index,",",IMD._columns(dsr)[oncols_right[i]][k])
    flag[cur_index+k] &= fun(x, r_col[k])
  end
end


function _find_count_for_left(flag, dsl_count, l_len, r_len)
  for i in 1:l_len
    lo = 1 + (i - 1) * r_len
    hi = lo + r_len - 1
    #push!(dsl_count,count(==(true),flag[lo:hi]))
    @inbounds dsl_count[i] = count(view(flag, lo:hi))
  end
end


function _fill_left_res(_res, l_col, dsl_count, new_ends, threads)
  # x = IMD._columns(dsl)[j] # 这一步可以隔离
  # 左侧填充
  IMD.@_threadsfor threads for i in eachindex(l_col)# 1:length(l_col)
    dsl_count[i] == 0 && continue   # = new_ends[i] - new_ends[i-1] = 0
    i == 1 ? lo = 1 : lo = new_ends[i-1] + 1
    hi = new_ends[i]
    IMD._fill_val_join!(_res, lo:hi, l_col[i])
  end
end


function _fill_right_res(_res, r_col, flag, r_len, threads)
  i, e = 1, lastindex(flag)
  cnt = 1
  while true
    res = findnext(flag, i)  #  找到i索引之后的下一个符合条件的索引，没填条件默认true
    isnothing(res) && break # 找不到则break
    # j = true || isempty(r) ? first(r) : last(r) 有ranges的才要判断
    #@inbounds i = nextind(flag, j)  ## 纯纯的找下一个索引的位置
    indx = res % r_len == 0 ? r_len : res % r_len # r_len
    _res[cnt] = r_col[indx]

    #println(i,r)
    i = res + 1
    cnt += 1
    i > e && break
  end
  """
  IMD.@_threadsfor threads for i in eachindex()#1:length(dsr_idx) # 1:length(l_len)  # left 的每一列
    _res[i] = r_col[dsr_idx[i]]
  end
  """
end

function _get_right_res_all(r_col, l_len)
  repeat(r_col, l_len)


  # 传 _res, l_len, r_len
  # for i in l_len, _res的i-i+r_len那么长 内容就是r_col，
  # lo= 1 + (i - 1) * r_len; hi = lo + r_len - 1
end
