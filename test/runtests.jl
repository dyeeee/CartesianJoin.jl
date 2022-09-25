using CartesianJoin
using Test

@testset "CartesianJoin.jl" begin
  @test CartesianJoin._cartesianjoin_test() == 1
  #@test CartesianJoin.hello_my_pkg() == "Hello CartesianJoin!"
  #@test CartesianJoin.main1() == "main"
end
