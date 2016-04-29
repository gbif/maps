
#Map cardinality
Historically GBIF map tile building services have been consistent on all content types and used a _brute force_ approach.  This results in a large amount of processing and aggregation for data views which may contain only a few 100 records.  Can we be smarter about determining what needs to be processed into a tile pyramid and what can simply be calculated at runtime from source locations?

Considering that we wish to provide a year time slider with the maps, and the ability to show maps by the basis of record (observation, specimen) etc. we can consider the feature as the combination of _latitude + longitude + basisOfRecord + year_.    

For each map type (e.g. Dataset) we calculate the distinct number of _features_ and bin them into categories of <1000, 1000-9999 etc. as follows.

TYPE       |   0-999 | 1000-9999 | 10000-99999 |  100000+ 
-----------|--------:|----------:|------------:|--------:
Dataset    |  12347  |       947 |         308 |       38
Kingdom    |       1 |         1 |           3 |        5      
Phylum     |      58 |        14 |          18 |       13      
Class      |     182 |        52 |          55 |       27 
Order      |     695 |       298 |         232 |      108
Family     |   12104 |      1908 |         778 |      215         
Genus      |  189943 |      8738 |        1916 |      351        
Species    | 1173312 |     14872 |        2289 |      367   
Taxon      | 1987063 |    16687  |	      2454 |      363
Publisher  |     340 |       232 |         155 |       44      
Country    |      36 |       125 |          66 |       26
H. Country |      18 |        15 |          14 |       23 

What this shows us is that for *very many* of our map types, there are actually less than 1000 features to render.   

> Perhaps it is possible to store those 1000 (or even 10,000) features without building the tile pyramid?

Doing such an approach would mean:

 - Storing the latitude, longitude, year and count for each map type for the first 1000(or more) records
 - Building tile pyramids for only those where the threshold is breached
 - Reprojections of the smaller numeric counts can be done _on-the-fly_
 - As we update datasets during indexing, some records will breach the threshold and then become batch oriented approaches

This seems beneficial and might mean we can offer richer mapping services and more regularly rebuild.  

