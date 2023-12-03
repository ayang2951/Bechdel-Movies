# Bechdel Project README

## Overview of Project

The Bechdel test, originating from the works of Alison Bechdel, was proposed as a thought experiment for measuring female representation in films and fictional productions. The test comprises three criteria:
1. Presence of More Than Two Female Characters: The film must feature at least two named female characters.
2. Conversation Between Female Characters: These characters should engage in a dialogue with each other.
3. Discussion Topics Excluding Men: Their conversation must revolve around a subject matter that is not centered on men.

Our project merges the Bechdel test with a comprehensive analysis of movie ratings, movie popularity, and the gender of directors and writers. While not intended as a standardized metric for measuring the importance of female characters in movies, this project aims to quantify the correlation between a film's success and the level of female involvement across its production, presentation, and reception.

## Objectives

Integrate the Bechdel test criteria into an analysis framework for films.
Examine correlations between Bechdel test passage, movie ratings, number of votes, and producers’ gender.
Highlight the influence of female representation on a film's success metrics.

## Datasets

We have acquired two primary datasets:

1. Bechdel Test Dataset: A collection of over 10,000 films spanning several decades, indicating movies’ titles, imdb id, test scores, score submission date, and dubious level of scores.
2. IMDb movies Dataset: Includes comprehensive film data such as runtime, director, genre, IMDB ratings, and more, enabling an analysis of various film aspects in relation to the Bechdel test results.

## Data Processing

### Bechdel Dataset + Joint Analysis

Producing Results along with the joint dataset for **Bechdel Test Analysis** & **Joint Analysis** from submitted zip
1. Use `bechdel.csv` from `bech_analysis`
2. Run java files on `bechdel.csv`to get an overview of the dataset.
3. Run `Clean.java` MapReduce job from `cleaning` on `bechdel.csv`
4. Perform code analysis on the cleaned output using `codeAnalysis.scala` through apache spark. 
5. Join `bechdel.csv` and `movie_data.csv` using `joint.scala`, which will
    - Join and output two datasets with selected columns.
    - Cast columns to data types of our interests.  The output is `joint.csv`
    - Perform individual analysis on `bechdel.csv`
    - Perform joint analysis on the joined dataset.
6. See visualization of distributions in `joint.csv` using `visualization.ipynb`


File Structure in HDFS:

Ingestion 
- IN HDFS (user tz2089_nyu_edu): **hw7/input/bechdel_movies_test.csv**
- Ingestion done using python by calling bechdeltest.com API

ETL
- ETL input data is **hw7/input/bechdel_movies_test.csv**, also available in Bechdel-Movies/profiling_code/bechdel/data/bechdel_movies.csv. Test data is Bechdel-Movies/profiling_code/bechdel/data/bechdel_movies_test.csv.
- Clean.java is in Bechdel-Movies/etl_code/bechdel/
- Clean.java cleans the data by testing integrity of the data, dropping useless columns, duplicates, and formatting fields. 
- This data is output to **hw7/output/part-r-00000*, as well as stored in Bechdel-Movies/ana_code/bech_analysis/bechdel.csv

Profiling
- Profiling original input data is **hw7/input/bechdel_movies_test.csv**
- RecCount:
    - Located in Bechdel-Movies/profiling_code/bechdel/RecCount
    - Counts the number of records before and after cleaning.
    - Screenshots in Bechdel-Movies/profiling_code/bechdel/RecCount/Screenshots/
- UniqueRec:
    - Located in Bechdel-Movies/profiling_code/bechdel/UniqueRec
    - Output an overview of the records with title, bechdel score, and dubious score. Execution in  Bechdel-Movies/profiling_code/bechdel/UniqueRec/Screenshots
- More profiling using spark in Bechdel-Movies/ana_code/bech_analysis/codeAnalysis.scala, statistical output in screenshots file. 


### IMDb Dataset + Joint Analysis

Producing Results for **IMDb Analysis** & **Machine Learning Models** from Submitted zip:

1. Use `imdb_data.csv` from `Bechdel-Movies/data_ingest/imdb` and `bechdel_data.csv` from `Bechdel-Movies/data_ingest/bechdel`
2. Run `Clean.java` MapReduce job from `Bechdel-Movies/etl_code/imdb` on `imdb_data.csv`, I will call this output `imdb_clean.csv`
3. Run profiling code on `imdb_clean.csv`:
    - Run `RatingsStats.java` from `Bechdel-Movies/profiling/imdb/ratingsstats` on this data
    - Run `YearCounts.java` from `Bechdel-Movies/profiling/imdb/yearcounts` on this data
4. Process data for machine learning models
    - Run `MLProcessing.java` from `Bechdel-Movies/ana_code/ml_code/process_imdb` on `imdb_clean.csv`, store this data as `imdb_ml_data.csv`
    - Run `Clean.java` from `Bechdel-Movies/ana_code/ml_code/process_bechdel` on `bechdel_data.csv`, store this data as `bechdel_clean.csv`
5. Start PySpark
    - Use two cleaned datasets, run `Bechdel-Movies/ana_code/ml_code/bechdel_ml.py`

File Structure in HDFS:

Ingestion
- IN HDFS (user awy2006_nyu_edu): **BechdelProject/input_data/imdb_data.csv** 
- Ingestion done using an R API
- Saved as csv files, two versions: one for categorical analysis, one for machine learning 
- Bolded filepath used for IMDb analysis.

ETL
- ETL input data is **BechdelProject/input_data/imdb_data.csv**
- `Clean.java` is in BechdelProject/ProjectCode/etl_code
    - `Clean.java` cleans the data by parsing the csv and changing the delimiters of column values
    - This data is saved in the BechdelProject/clean_data directory, then its name is changed from “part-r-00000” to “imdb_clean.csv”
    - ETL output data is **BechdelProject/clean_data/imdb_clean.csv**

Profiling
- Profiling input data is **BechdelProject/clean_data/imdb_clean.csv**
- RatingsStats.java:
    - `RatingsStats.java` is in `BechdelProject/ProjectCode/profiling/ratingsstats`
    - Calculates basic statistics for ratings through the decades
    - Output file is **BechdelProject/profiling/ratings_stats/part-r-00000**
- YearCounts.java:
    - `YearCounts.java` is in `BechdelProject/ProjectCode/profiling/year_counts`
    - Counts the number of films from each year in the dataset
    - Output file is **BechdelProject/profiling/year_counts/part-r-00000**

Further Data Processing for Machine Learning
- Further data processing was needed for both datasets for the machine learning component of this project
- Bechdel dataset processing:
    - Input data is **BechdelProject/input_data/bechdel_data.csv*
    - Clean.java is in `BechdelProject/ProjectCode/process_bechdel`
    - Clean.java parses the csv and extracts the two columns we need: the Bechdel rating and the imdbID for the join
    - Output data is in **BechdelProject/clean_bechdel/bechdel_clean.csv**
- IMDb dataset processing:
    - Input data is **BechdelProject/clean_data/imdb_clean.csv**
    - `MLProcessing.java` is in `BechdelProject/ProjectCode/ml`
    - `MLProcessing.java` subsets the columns that we need for the models, and removes nulls
    - Output data is in **BechdelProject/imdb_ml/imdb_ml_data.csv**

Machine Learning Done in PySpark
    - File containing model building & output is in `BechdelProject/script`

## References

### DataSource
1. Bechdel Test Dataset: https://bechdeltest.com/
2. IMDb Film Metrics Dataset: `omdbapi` in R

### Literature
	1.https://aclanthology.org/N15-1084.pdf
	2.INTERNATIONAL JOURNAL of ARTS MANAGEMENT