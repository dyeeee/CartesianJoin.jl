#包的一级入口

module CartesianJoin

#using InMemoryDatasets


include("main.jl")   # 主函数入口





function __init__()
  if Threads.nthreads() == 1
    if get(ENV, "IMD_WARN_THREADS", "1") == "1"
      @warn "Julia started with single thread, to enable multithreaded functionalities in InMemoryDatasets.jl start Julia with multiple threads."
    end
  end
end


export _cartesianjoin_test
end
