# hbase-backed-map
A simple java HashMap implementation backed by HBase. 

Maintains an internal HashMap<String, Long> and stores the string against a unique identifier. Keeps it in memory while during usage purposes. Internally, it persists it in HBase as well.

Upon insertion, it first looks over the memory and then to HBase for the string entity. If not present, it creates a new one with a unique identifier and keeps it in memory and persists it in HBase as well.

Primary intention is the use cases when string persistence becomes a true hurdle. (Eg:- Case of string handling in VoltDB)