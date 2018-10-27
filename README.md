# message-queue-client-framework-benchmark

![image](http://darkphoenixs.org/message-queue-client-framework/uml/benchmark.png)

#### 2 x ECS (AliCloud VM) for Producer and Consumer test. 
```
OS： CentOS 6.9 64-Bit
CPU： 4 Core
MEM： 8 GB
TYPE： Optimal IO
```
#### Test case scenarios
```
Data Size:  62 byte
Data Count:  1000W
Topic Partition:  10
Producer Concurrency:  10
Consumer Concurrency:  10
```

#### Producer with Kafka Native API.
```
send time 12.113 s. average send 825559.316436886/s.
send time 12.18 s. average send 821018.0623973728/s.
send time 12.226 s. average send 817929.0037624734/s.
send time 12.237 s. average send 817193.7566396992/s.
send time 12.246 s. average send 816593.1732810714/s.
send time 12.317 s. average send 811886.011204027/s.
send time 12.313 s. average send 812149.7604158206/s.
send time 12.345 s. average send 810044.5524503847/s.
send time 12.357 s. average send 809257.9104960752/s.
send time 12.392 s. average send 806972.2401549387/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 7407484 143056 401580    0    0     0     0  238  408  0  0 100  0  0
 3  0      0 7092292 143056 401612    0    0     0     7 8229 2811 70  3 27  0  0
 8  0      0 6774964 143056 401612    0    0     0    15 7417 3779 81  7 12  0  0
 4  0      0 6477824 143056 401612    0    0     0     0 8682 4357 82 12  6  0  0
 5  0      0 6423168 143056 401612    0    0     0     0 10401 6494 78 12 10  0  0
 8  0      0 6422300 143056 401612    0    0     0     0 12043 10475 68 15 17  0  0
 0  0      0 7406828 143056 401584    0    0     0     1 6600 6281 29  8 64  0  0
 0  0      0 7406876 143056 401584    0    0     0    17  200  366  0  0 100  0  0
```

#### Consumer with Kafka Native API.
```
receive time 12.515 s. average receive 799041.1506192569/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 6667656 143860 675964    0    0     0     0  541  799  0  0 100  0  0
 9  0      0 6537840 143860 675968    0    0     0     0 6467 2546 29  3 69  0  0
 0  0      0 6427632 143860 675968    0    0     0     0 12134 4246 35  5 60  0  0
 0  0      0 6324240 143860 675968    0    0     0     5 11772 4545 29  5 65  0  0
 1  0      0 6258388 143860 675968    0    0     0     1 13344 5546 21  5 74  0  0
 0  0      0 6257040 143860 675968    0    0     0     0 8944 3967 10  3 87  0  0
 0  0      0 6256916 143860 675972    0    0     0     7  566  777  0  0 99  0  0
```

#### Producer with Client Framework API.
```
send time 13.472 s. average send 742280.2850356295/s.
send time 13.511 s. average send 740137.6656058027/s.
send time 13.683 s. average send 730833.8814587444/s.
send time 13.777 s. average send 725847.4268708718/s.
send time 13.804 s. average send 724427.7021153289/s.
send time 13.801 s. average send 724585.1749873197/s.
send time 13.833 s. average send 722908.985758693/s.
send time 13.898 s. average send 719527.989638797/s.
send time 13.893 s. average send 719786.9430648527/s.
send time 14.0 s. average send 714285.7142857143/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 7407156 143056 401724    0    0     0     4  264  405  0  0 100  0  0
 4  0      0 7111608 143056 401756    0    0     0     7 7730 2328 60  3 36  0  0
 9  0      0 6650076 143056 401756    0    0     0    16 7345 3384 82  8 10  0  0
 6  0      0 6448448 143056 401756    0    0     0     0 8557 4698 79 11  9  0  0
 6  0      0 6391572 143056 401756    0    0     0     0 8345 3032 83 10  7  0  0
 7  0      0 6358308 143056 401760    0    0     0     1 9841 7700 74 12 13  0  0
 1  0      0 6344704 143056 401760    0    0     0    11 11229 9435 71 13 16  0  0
 0  0      0 7407048 143056 401728    0    0     0     0 1424 1271  1  1 98  0  0
```

#### Consumer with Client Framework API.
```
receive time 14.089 s. average receive 709773.5822272695/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 6676208 143860 676112    0    0     0     0  547  811  0  0 100  0  0
 5  0      0 6438288 143860 676116    0    0     0    11 6381 2733 30  4 66  0  0
 1  0      0 6250020 143860 676116    0    0     0    19 13449 3598 64  6 30  0  0
 0  0      0 6245056 143860 676116    0    0     0    23 11255 4001 42  4 53  0  0
 0  0      0 6238104 143860 676116    0    0     0     5 11998 4880 26  5 69  0  0
 1  0      0 6235436 143860 676116    0    0     0     0 12802 5227 27  5 68  0  0
 0  0      0 6233132 143860 676120    0    0     0     3 2338 1389  4  1 95  0  0
 0  0      0 6233008 143860 676120    0    0     0    11  529  752  0  0 100  0  0
```

#### Producer with Spring Cloud API.
```
send time 154.786 s. average send 64605.32606308064/s.
send time 154.826 s. average send 64588.63498378826/s.
send time 155.054 s. average send 64493.66027319514/s.
send time 155.054 s. average send 64493.66027319514/s.
send time 155.121 s. average send 64465.80411420762/s.
send time 155.14 s. average send 64457.90898543252/s.
send time 155.153 s. average send 64452.50816935542/s.
send time 155.202 s. average send 64432.15937938944/s.
send time 155.221 s. average send 64424.27248890292/s.
send time 155.458 s. average send 64326.0559122078/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 2  0      0 6129084 143056 401916    0    0     0     1 17097 26121 60  7 33  0  0
 7  0      0 6126540 143056 401916    0    0     0     0 17814 27176 58  8 35  0  0
 2  0      0 6084276 143056 401916    0    0     0     4 13804 21506 51  6 44  0  0
 4  0      0 6083904 143056 401920    0    0     0     0 13833 22601 41  6 53  0  0
 4  0      0 6083780 143056 401920    0    0     0     0 17088 25031 62  7 30  0  0
 3  0      0 6084028 143056 401920    0    0     0     9 17273 25230 62  8 31  0  0
 5  0      0 6081980 143056 401920    0    0     0     0 16675 24666 63  7 30  0  0
 4  0      0 6081856 143056 401920    0    0     0    17 16094 22026 61  8 31  0  0
 0  0      0 6038476 143056 401924    0    0     0     0 14764 19787 61  7 32  0  0
```

#### Consumer with Spring Cloud API.
```
receive time 301.986 s. average receive 33114.11787301398/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 1  0      0 6606928 143860 676264    0    0     0    16 2782 1441 35  1 64  0  0
 1  0      0 6469280 143860 676264    0    0     0     0 2495 1546 14  1 84  0  0
 1  0      0 6462136 143860 676264    0    0     0     5 2607 1509 19  1 80  0  0
 0  0      0 6295392 143860 676268    0    0     0     9 2097 1415 11  1 88  0  0
 1  0      0 6252448 143860 676268    0    0     0     0 1933 1437  6  1 93  0  0
 0  0      0 6252448 143860 676268    0    0     0     0 2142 1517  6  1 92  0  0
 0  0      0 6250836 143860 676268    0    0     0     7 2359 1515 12  1 87  0  0
 0  0      0 6249656 143860 676272    0    0     0     0 1820 1374  6  1 93  0  0
 0  0      0 6239172 143860 676272    0    0     0     3 2265 1464 10  1 89  0  0
```