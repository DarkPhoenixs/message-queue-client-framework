# message-queue-client-framework-benchmark

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

#### Producer with Native.
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
 0  0      0 7407484 143056 401580    0    0     0     2    0    0  1  0 99  0  0
 0  0      0 7407484 143056 401580    0    0     0     0  238  408  0  0 100  0  0
 3  0      0 7092292 143056 401612    0    0     0     7 8229 2811 70  3 27  0  0
 8  0      0 6774964 143056 401612    0    0     0    15 7417 3779 81  7 12  0  0
 4  0      0 6477824 143056 401612    0    0     0     0 8682 4357 82 12  6  0  0
 5  0      0 6423168 143056 401612    0    0     0     0 10401 6494 78 12 10  0  0
 8  0      0 6422300 143056 401612    0    0     0     0 12043 10475 68 15 17  0  0
 0  0      0 7406828 143056 401584    0    0     0     1 6600 6281 29  8 64  0  0
 0  0      0 7406876 143056 401584    0    0     0    17  200  366  0  0 100  0  0
 0  0      0 7407048 143056 401584    0    0     0     0  188  361  0  0 100  0  0
 0  0      0 7407048 143056 401584    0    0     0     1  196  363  0  0 100  0  0
```

#### Consumer with Native.
```
receive time 12.515 s. average receive 799041.1506192569/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 6665176 143860 675964    0    0     0     2    0    0  1  0 98  0  0
 1  0      0 6665796 143860 675964    0    0     0     9  797  787  5  0 95  0  0
 0  0      0 6667656 143860 675964    0    0     0    12  554  758  1  0 99  0  0
 0  0      0 6667656 143860 675964    0    0     0     0  541  799  0  0 100  0  0
 9  0      0 6537840 143860 675968    0    0     0     0 6467 2546 29  3 69  0  0
 0  0      0 6427632 143860 675968    0    0     0     0 12134 4246 35  5 60  0  0
 0  0      0 6324240 143860 675968    0    0     0     5 11772 4545 29  5 65  0  0
 1  0      0 6258388 143860 675968    0    0     0     1 13344 5546 21  5 74  0  0
 0  0      0 6257040 143860 675968    0    0     0     0 8944 3967 10  3 87  0  0
 0  0      0 6256916 143860 675972    0    0     0     7  566  777  0  0 99  0  0
 1  0      0 6257040 143860 675972    0    0     0     3  517  782  0  0 100  0  0
 0  0      0 6257040 143860 675972    0    0     0     8  501  772  0  0 100  0  0
```

#### Producer with Client.

#### Consumer with Client.

#### Producer with Spring.

#### Consumer with Spring.
