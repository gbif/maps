/**
 * The MapMetastore provides utilities for rotating out tables as new versions are generated.
 *
 * The Zookeeper implementation stores the metadata locating the current live tables and the date of their generation.
 * This metastore is required since we make use of offline HFile generation which are bulkloaded periodically.
 * The bulk loading cannot be done on the live table because there is no way to determine cell deletion.
 *
 * Tables are of the structure <env>_<prefix>_<YYYYMMDDHHSS>.  E.g. prod_a_map_tiles_201707011312
 *
 * Implementation details are contained in this package and a builder exists to allow construction.
 */
package org.gbif.maps.common.meta;


