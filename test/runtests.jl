using CartesianJoin
using Test

@testset "CartesianJoin.jl" begin
  @test CartesianJoin.sum_values(2, 2) == 4
  @test CartesianJoin.hello_my_pkg() == "Hello CartesianJoin!"
  @test CartesianJoin.main1() == "main"
end
