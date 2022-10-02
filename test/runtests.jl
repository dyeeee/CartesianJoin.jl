using CartesianJoin
using Test
using InMemoryDatasets

d1 = Dataset(ID=Union{Int,Missing}[1, 2, 3],
  Name=Union{String,Missing}["John Doe", "Jane Doe", "Joe Blogs"])

d1 = Dataset(ID=Union{Int,Missing}[1, 2, 3],
  Name=Union{String,Missing}["John Doe", "Jane Doe", "Joe Blogs"])



@testset "CartesianJoin.jl" begin
  @test CartesianJoin.cartesianjoin() == 1
  #@test CartesianJoin.cartesianjoin(d1) == d1
  #@test CartesianJoin.main1() == "main"
end
