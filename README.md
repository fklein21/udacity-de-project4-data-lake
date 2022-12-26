<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->


# Project 4: Data Lake

<p style='text-align: right;'> 26/12/22 </p>

### Udacity Bosch AI Talent Accelerator Scholarship
### Ferdinand Kleinschroth
(ferdinand.kleinschroth@gmx.de)
<div align="center"> 

![](images/Cloud_Data_Warehouses-Udacity.png)
</div>



<!-- ABOUT THE PROJECT -->
## About The Project

The objective of the project was to create an ETL pipeline to extract their data from AWS S3 buckets, process them using Spark to create dimensional tables and move the processed data back to S3 buckets.

The data consists of:
- a song library in the form of a collection of json files with information about each song and
- log data about user activity, also in the form of json files.

The processed data was saved in the S3 bucket in the parquet format. It is in a star schema, saved in the following files:
- `songplays.parquet` (fact table): Contains all entries of the log files which are associated with a song play. The columns are   
  - `songplay_id`, 
  - `start_time`, 
  - `user_id`, 
  - `level`, 
  - `song_id`, 
  - `artist_id`, 
  - `session_id`, 
  - `location`, 
  - `user_agent`
- `users.parquet` (dimension table): Contains user information.
  - `user_id`, 
  - `first_name`, 
  - `last_name`, 
  - `gender`, 
  - `level`
- `songs.parquet` (dimension table): Contains information about the songs in the song library.
  - `song_id`, 
  - `title`, 
  - `artist_id`, 
  - `year`, 
  - `duration`
- `artists.parquet` (dimension table): Contains information about the artists.
  - `artist_id`, 
  - `name`, 
  - `location`, 
  - `latitude`, 
  - `longitude`
- `time.parquet` (dimension table): Timestamps of each log event split up into hour, day, ...
  - `start_time`, 
  - `hour`, 
  - `day`, 
  - `week`, 
  - `month`, 
  - `year`, 
  - `weekday`



## How to run the `etl.py` script

The script needs to be run on a AWS EMR cluster with the configuration as specified in the project instructions on the Udacity website.

The following parameters have to be specified:
- input_file: The location (s3_bucket) of the input files.
- output_file: The location (s3_bucket) to which the output should be written.

## Files in the repository

- `etl.py` : Python script that processes the data as described above.
- `etl.ipynb`: Jupyter notebook to develop the pipeline before putting the logic in the Python script.
- `README.md` : This readme file

## Discussion of future improvements

In the data processing step no analysis of data skewness was done.
The data is partitioned in year and month (songplays and time) and year and artist_id (songs), respectively. 
The data could be skewed in several ways:
- The user base has grown a lot in the past. There are much more songplay events in more recent years.
- Some artists may accumulate a large part of the songplays (songs by very famous artists), whereas the majority of artists have only very few songplays.
- The release date of songs may not be equally distributed of the years.

In a future release this should be taken into account when deciding on a partitioning strategy.



## Acknowledgments

- The template for the README was taken from [Best-README-Template (GitHub repo)](https://github.com/othneildrew/Best-README-Template).
- The image at the top of the file was taken from the Udacity website.
