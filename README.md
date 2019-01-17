# cafe
This is the library component of the OLTP-Bench implementation of TPC-C that uses client-side cache in the write-back mode.  It must be used with:
1.  Version of memcached that supports S and X leases along with the concept of pinning buffered writes, https://github.com/scdblab/IQ-WhalinTwemcache
2.  OLTP-Bench implementation of TPC-C, https://github.com/scdblab/OLTPBench-WriteBack-ClientSideCache

This project was used to gather performance numbers reported in:  Shahram Ghandeharizadeh and Hieu Nguyen.  [Design, Implementation, and Evaluation of Write-Back Policy with Cache Augmented Data Stores.](http://dblab.usc.edu/Users/papers/writeback.pdf)  USC Database Laboratory Technical Report Number 2018-07.
