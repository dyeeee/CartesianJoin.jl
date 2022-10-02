using CartesianJoin
using Test
using InMemoryDatasets

dsl = Dataset(xid=[111, 222, 333, 444, 222],
  x1=[1, 2, 1, 4, 3],
  x2=[-1.2, -3, 2.1, -3.5, 2.8],
  x3=[Date("2019-10-03"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")],
  x4=["abcd", "efgh", "ijkl", "mnop", "qrst"]);

dsr = Dataset(yid=[111, 111, 222, 444, 333],
  y1=[3, 3, 3, 3, 3],
  y2=[0, 0, -3, 1, 2],
  y3=[Date("2019-10-01"), Date("2019-10-01"), Date("2019-09-30"), Date("2019-10-05"), Date("2019-10-05")],
  y4=["abc", "abcd", "efg", "mnop", "qrst"]);

@testset "CartesianJoin.jl" begin
  @test CartesianJoin.cartesianjoin(dsl, dsr, on=[:xid => :yid]) == innerjoin(dsl, dsr, on=[:xid => :yid])
  @test CartesianJoin.cartesianjoin(dsl, dsr, on=[:xid => :yid, :x1 => :y1]) == innerjoin(dsl, dsr, on=[:xid => :yid, :x1 => :y1])
  #@test CartesianJoin.main1() == "main"
end
