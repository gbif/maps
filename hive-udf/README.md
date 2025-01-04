## Hive UDFs for coordinate projections

Contains Hive UDFs that use existing Java code for projecting coordinates.
(A future version of this would be done in Spark)

Example of use, where the data is projected to a global coordinate reference at zoom 16.
Copy the Jar to the c5gateway `/tmp` and then:

- See [Mercator instructions](./mercator.md)
- See [WGS84 instructions](./wgs84.md)
- See [Arctic instructions](./arctic.md)
- See [Antarctic instructions](./antarctic.md)

_These are similar differing in table naming and the `WHERE` clause only_
