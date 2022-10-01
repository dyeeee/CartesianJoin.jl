function hello_my_pkg()
  return "Hello CartesianJoin!"
end


function sum_values(x, y)
  return x + y
end


Base.@propagate_inbounds function _fill_val_join!(x, r, val)
  @simd for i in r
    x[i] = val
  end
end


function _get_dsr_idx(flag, dsl_count, l_len, r_len, new_ends)
  dsr_idx = findall(BitVector(flag))
  for i in 1:l_len
    dsl_count[i] == 0 && continue

    i == 1 ? lo = 1 : lo = new_ends[i-1] + 1
    hi = new_ends[i]

    dsr_idx[lo:hi] .-= (i - 1) * r_len
  end

  dsr_idx
end


function _create_multiple_match_col_cartesian(dsl_count, en, total_length)
  res = IMD.allocatecol(Bool, total_length)
  cnt = 0
  # en to handle range, -- 到时候看看我这样会不会有什么问题
  if en === nothing
    for i in 1:length(dsl_count)
      if dsl_count[i] == 0
        nothing
      else
        if dsl_count[i] == 1
          cnt += 1
          res[cnt] = false
        else
          for j in 1:dsl_count[i]
            cnt += 1
            res[cnt] = true
          end
        end
      end
    end
  end

  res
end


