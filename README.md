# CartesianJoin

[![Build Status](https://github.com/dyeeee/CartesianJoin.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/dyeeee/CartesianJoin.jl/actions/workflows/CI.yml?query=branch%3Amain)


# Introduction

[CartesianJoin.jl]() is a join extension package developed based on the high-performance data processing package [InMemoryDatasets](https://github.com/sl-solution/InMemoryDatasets.jl), which expands the `cartesianjoin` function of the `Dataset` object. The performance of this package was developed according to the requirements of IMD, including minimal allocations and high operating speeds.

# Features


# Examples


# Benchmark



## Version history

### Beta

#### v1 
Basic ideas. Using Flag matrix.

#### v1t
Multi-threading.

#### v2
Vector operation refactoring.

#### v3
Better function barriers to further optimize vector operations. 

#### v4
Flag **vector** replaced Flag matrix.

#### v5
Better function barriers when computing flag and generating new datset.

#### v6
Remove findall funtion to minimize allocations.

### First release 1.0

1.0.1 - Split a join with a timer for performance evaluation.

1.0.2 - All arguments from IMD.join is supported.