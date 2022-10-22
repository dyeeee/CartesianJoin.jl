# CartesianJoin

[![Build Status](https://github.com/dyeeee/CartesianJoin.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/dyeeee/CartesianJoin.jl/actions/workflows/CI.yml?query=branch%3Amain)


# Introduction

[CartesianJoin.jl]() is a join extension package developed based on the high-performance data processing package [InMemoryDatasets](https://github.com/sl-solution/InMemoryDatasets.jl), which expands the `cartesianjoin` function of the `Dataset` object. The performance of this package was developed according to the requirements of IMD, including minimal allocations and high operating speeds.

# Features

1. Supports arbitrary user-defined Boolean functions as join conditions. 
	```{Julia}
    function fun(left_x, right_x)
      # user-defined operations
      return true/false
    end
    ```
    This also means that not only inequal join or range join, but also arbitrary conditional join.

2. Any number of columns are supported using arbitrary conditional associations.

3. Enabling multi-threading for acceleration, the performance better than implementing the same data operations in R.

4. Very few allocations, other than the necessary auxiliary variables, do not produce any additional allocations.

5. Support all IMD Dataset index column names are supported, user can use symbol, string, index to indicate columns then join two datasets.

6. All parameters related to the join in IMD are supported, including `mapdformats`, `multiple_match` (whether the display is a duplicate match), `obs_id` (displaying the index in the original data set), etc.

7. Use `cartesianjoin_timer(dsl,dsr,on = [:x1=>:y1, :x2=>:y2=>func])` to check the time and memory consumption of each process.

# Examples

```{julia}
using Pkg
Pkg.add("CartesianJoin")

dsl = Dataset(xid = [111,222,333,444,222], 
              x1 = [1,2,1,4,3], 
              x2 = [-1.2,-3,2.1,-3.5,2.8],
              x3 = [Date("2019-10-03"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")],
              x4 = ["abcd","efgh","ijkl","mnop","qrst"]);

dsr = Dataset(yid = [111,111,222,444,333],
              y1 = [3,3,3,3,3],
              y2 = [0,0, -3,1,2],
              y3 = [Date("2019-10-01"),Date("2019-10-01"), Date("2019-09-30"), Date("2019-10-05"), Date("2019-10-05")],
              y4 = ["abc","abcd","efg","mnop","qrst"]);

function fun1(x,y) 
  x <= y
end

function fun2(x,y) 
  x >= y
end

function fun3(x,y) 
  length(x) == length(y)
end

# Return cartesian join result.
newds = cartesianjoin(dsl,dsr);

# Cartesian join with conditions. Default function is `isequal()`.
newds = cartesianjoin(dsl,dsr,on = [:xid=>:yid, :x1=>:y1=>fun1]);

# Cartesian join with multi user defined conditions.
newds = cartesianjoin(dsl,dsr,
          on = [:xid=>:yid=>fun2, :x1=>:y1=>fun1, :x4=>:y4=>:fun4],
          multiple_match=[true,false],
          obs_id=[true,false]);
```

# Benchmark

* *size* for dataset: the rows number for both right and left dataset.  All cols number are 5 (type: Int, Int, String, Float, Date).

* *machine* : Mac with 2.9 GHz quad-core Intel Core i7 and 16 GB memory. Thread for Julia and R data.table is set to be 8.

## 1. Cartesianjoin without on conditions. 
| size\method | crtesianjoin() | R data.table | R data.frame | Python pandas |
|:-----------:|:--------------:|:------------:|:------------:|:-------------:|
|     1e2     |     0.95 ms    |    4.89 ms   |   10.16 ms   |    4.49 ms    |
|     1e3     |    29.90 ms    |   65.69 ms   |    2.57 s    |   177.90 ms   |
|     1e4     |     5.71 s     |    9.15 s    |       /      |    24.36 s    |

## 2. Inequality join for multiple cols

```{julia}
@benchmark cartesianjoin(dsl_1e3,dsr_1e3,
              on = [:x1 => :y1 => isless, :x2 => :y2 => isless, :x4 => :y4 => isless],
              threads = true, check = false)
```
| size\method | crtesianjoin() | R data.table |
|:-----------:|:--------------:|:------------:|
|     1e2     |     0.70 ms    |    4.24 ms   |
|     1e3     |    21.39 ms    |   29.12 ms   |
|     1e4     |     2.16 s     |    1.92 s    |


## 3. Inequality join for multiple cols
for data.table, do crossjoin and filter by conditions

```{Julia}
function str_match(x,y)
  return x[8] === y[8]
end

@benchmark cartesianjoin(dsl_1e3,dsr_1e3,
                    on = [:x3 => :y3 => str_match],
                    threads = true, check = false)
```

| size\method | crtesianjoin() | R data.table |
|:-----------:|:--------------:|:------------:|
|     1e2     |     0.50 ms    |    6.83 ms   |
|     1e3     |    11.51 ms    |   173.36 ms  |
|     1e4     |     1.13 s     |    19.18 s   |

# Version history

## First release 0.1

0.1.3 - Normal Cartesianjoin supported.

0.1.2 - All arguments from IMD.join is supported.

0.1.1 - Split a join with a timer for performance evaluation.


## Beta

v1 - Basic ideas. Using Flag matrix.

v1t - Multi-threading.

v2 - Vector operation refactoring.

v3 - Better function barriers to further optimize vector operations. 

v4 - Flag **vector** replaced Flag **matrix**.

v5 - Better function barriers when computing flag and generating new datset.

v6 - Remove findall funtion to minimize allocations.

