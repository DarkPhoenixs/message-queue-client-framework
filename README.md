# message-queue-client-framework-benchmark

![image](http://darkphoenixs.org/message-queue-client-framework/uml/benchmark.png)

#### 2 x ECS (AliCloud VM) for Producer and Consumer test. 
```
OS： CentOS 6.9 64-Bit
CPU： 4 Core 2.5 GHz
MEM： 8 GB
Network:  1.5 Gbps
```
#### Test case scenarios
```
Data Size:  62 bytes
Data Count:  1000W
Topic Partition:  10
Producer Concurrency:  10
Consumer Concurrency:  10
```

#### Producer with Kafka Native API.
```
send time 11.638 s. average send 859254.1673827118/s.
send time 11.753 s. average send 850846.5923593976/s.
send time 11.879 s. average send 841821.7021634819/s.
send time 11.91 s. average send 839630.5625524769/s.
send time 11.934 s. average send 837942.0144126026/s.
send time 11.936 s. average send 837801.6085790885/s.
send time 11.959 s. average send 836190.3169161301/s.
send time 11.98 s. average send 834724.5409015025/s.
send time 12.001 s. average send 833263.8946754438/s.
send time 12.016 s. average send 832223.7017310252/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 7409748 143092 400200    0    0     0     7  259  425  0  0 100  0  0
 3  0      0 7069888 143092 400232    0    0     0     1 8319 2566 69  3 27  0  0
 7  0      0 6730544 143092 400232    0    0     0    15 8221 4073 78  9 13  0  0
 8  0      0 6447136 143092 400236    0    0     0     0 8417 3949 81 12  7  0  0
 7  0      0 6429504 143092 400236    0    0     0     0 10105 7328 75 13 13  0  0
 3  0      0 6436832 143092 400236    0    0     0     7 11724 9839 69 15 16  0  0
 0  0      0 7409452 143092 400204    0    0     0     0 2292 2513  7  2 91  0  0
```

#### Consumer with Kafka Native API.
```
receive time 11.997 s. average receive 833541.7187630241/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 6232152 143872 684764    0    0     0     8  551  807  0  0 100  0  0
 0  0      0 6232028 143872 684764    0    0     0     7 7015 3704 10  2 87  0  0
 3  0      0 6228432 143872 684768    0    0     0     7 11814 5730 11  4 85  0  0
 1  0      0 6224276 143872 684768    0    0     0     0 11134 5625 11  4 85  0  0
 0  0      0 6249640 143872 684768    0    0     0     0 12495 6099 13  4 83  0  0
 0  0      0 6258948 143872 684768    0    0     0     0 3522 2079  4  1 95  0  0
 0  0      0 6258948 143872 684768    0    0     0     5  541  798  0  0 100  0  0
```

#### Producer with Client Framework API.
```
send time 12.23 s. average send 817661.4881439084/s.
send time 12.323 s. average send 811490.7084313885/s.
send time 12.504 s. average send 799744.081893794/s.
send time 12.537 s. average send 797638.9885937624/s.
send time 12.576 s. average send 795165.3944020356/s.
send time 12.645 s. average send 790826.4136022143/s.
send time 12.778 s. average send 782595.0853028643/s.
send time 12.796 s. average send 781494.2169427946/s.
send time 12.843 s. average send 778634.2754808067/s.
send time 12.857 s. average send 777786.4198491095/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 7410188 143092 400100    0    0     0     9  222  402  0  0 100  0  0
 2  0      0 7163872 143092 400132    0    0     0     1 6850 2254 45  2 53  0  0
 5  0      0 6835568 143092 400136    0    0     0     1 5944 2995 78  4 18  0  0
 3  0      0 6456924 143092 400136    0    0     0    17 8507 4349 77 13 10  0  0
 6  0      0 6420904 143092 400136    0    0     0    19 8204 4388 82 10  8  0  0
 3  0      0 6399492 143092 400136    0    0     0     0 10431 9348 73 13 14  0  0
 0  0      0 7409808 143092 400136    0    0     0     4 10709 9182 62 13 25  0  0
 0  0      0 7409828 143092 400108    0    0     0     0  217  389  0  0 100  0  0
```

#### Consumer with Client Framework API.
```
receive time 12.585 s. average receive 794596.7421533571/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 0  0      0 6233176 143872 684692    0    0     0     5  545  804  0  0 100  0  0
 1  0      0 6233424 143872 684692    0    0     0     0  640  873  0  0 100  0  0
 0  0      0 6243036 143872 684692    0    0     0     0 10609 4537 21  4 75  0  0
 0  0      0 6239680 143872 684696    0    0     0     1 8662 3820 15  3 81  0  0
 3  0      0 6228504 143872 684696    0    0     0    12 12012 4814 23  5 71  0  0
 0  0      0 6228752 143872 684696    0    0     0     0 13187 5175 28  6 66  0  0
 0  0      0 6228876 143872 684696    0    0     0    16 3421 1836  6  1 92  0  0
 0  0      0 6229000 143872 684696    0    0     0     0  501  776  0  0 100  0  0
```

#### Producer with Spring Cloud API.
```
send time 71.148 s. average send 140552.08860403666/s.
send time 71.465 s. average send 139928.6363954383/s.
send time 71.524 s. average send 139813.20955203846/s.
send time 71.523 s. average send 139815.1643527257/s.
send time 71.565 s. average send 139733.1097603577/s.
send time 71.919 s. average send 139045.31486811553/s.
send time 72.123 s. average send 138652.0250128253/s.
send time 72.144 s. average send 138611.66555777332/s.
send time 72.178 s. average send 138546.3714705312/s.
send time 72.248 s. average send 138412.13597608238/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 5  0      0 6701196 143092 400056    0    0     0    17 7247 2208 93  5  3  0  0
 9  0      0 6538916 143092 400060    0    0     0     0 7494 2143 90  7  3  0  0
13  0      0 6517960 143092 400060    0    0     0     0 8314 2082 90  8  2  0  0
10  0      0 6508224 143092 400060    0    0     0     4 8025 1991 90  7  4  0  0
 8  0      0 6486252 143092 400060    0    0     0     3 8181 2151 90  7  2  0  0
10  0      0 6476196 143092 400060    0    0     0     9 8388 2185 88  9  3  0  0
 6  0      0 6476568 143092 400064    0    0     0     0 8542 2309 89  9  3  0  0
 4  0      0 6474644 143092 400064    0    0     0     1 8661 2396 88  9  3  0  0
10  0      0 6474644 143092 400068    0    0     0    31 9155 2592 88  9  3  0  0
```

#### Consumer with Spring Cloud API.
```
receive time 71.951 s. average receive 138983.4748648386/s.
```
```
procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
 r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 9  0      0 6120920 143872 684616    0    0     0     8 9802 4008 33  6 61  0  0
 0  0      0 6121308 143872 684616    0    0     0     8 10771 4071 45  8 47  0  0
 3  0      0 6120936 143872 684616    0    0     0     0 10621 3942 43  7 50  0  0
 5  0      0 6121556 143872 684616    0    0     0     0 10820 3872 46  7 46  0  0
 0  0      0 6121308 143872 684616    0    0     0     0 12182 4189 45  8 47  0  0
 5  0      0 6120688 143872 684620    0    0     0     1 11693 4029 45  8 47  0  0
 6  0      0 6120068 143872 684620    0    0     0    12 12529 4112 46  8 45  0  0
 7  0      0 6120068 143872 684620    0    0     0     0 12210 4023 45  8 47  0  0
 4  0      0 6120564 143872 684620    0    0     0    11 12071 3999 45  8 47  0  0
```
