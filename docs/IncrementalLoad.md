# Incremental Load

Pipelines are generally designed to load and process only the data that has changed between the current run and the previous run. And this is generally called the Incremental Load.

The 'day one' files are commonly known in the industry as the cutover file or the history file, and the subsequent files are sometimes referred as Delta files,

### Hybrid Scenarios

- We can receive a full dataset but still choose to process them incrementally.
- We can receive an incremental data set but process that in full.
- In some cases we could receive some files within a data set as full and some as incremental, and our process will have to deal with those.
- In some cases we might receive incremental data and ingested them incrementally, but transformations are done in full.



#### Hybrid - some as full and some as incremental
One solution is to develop two sets of files, one to develop a full refresh for the cut over file or the Day 1 file, and then create incremental loads for Day 2, Day 3, ...  
This has been done in the past for many projects in the industry, but this is not the best approach I would advise. As this will require two lots of code testing, implementation and quite a lot of cost involved in terms of maintaining two pieces of code.  

A better solution is to implement an incremental load for all of these files, including the Day 1 file, because in theory these are all incremental data, just that the first day's data has more than one day's race within that one. The one only thing to be careful of is we will have an incremental load pattern being implemented for this one. But our incremental load code would be clever enough to understand that one of the days may have more
than one race worth of data.  

Ingestion process will be need to be smarter. It will be fed with the race date as the parameter. It gets the list of race IDs from a particular subfolder in raw using the race date supplied. Data for those race IDs are then removed from the process container in our Data Lake and new data is then loaded.  

Keeping the subfolder name as race date gives us the ability to process data just from that race. Identifying the unique race IDs within the file gives us the ability to deal with the cutover file more effectively because that will have more race IDs as well. The transformation process does a very similar process. It identifies the race IDs to be processed, gets the data from our process container, does the transformation and updates our Data Lake presentation layer.

