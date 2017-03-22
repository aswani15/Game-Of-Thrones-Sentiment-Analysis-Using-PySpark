Abstract:


The Game of Thrones series, based on the book – A Song of Ice and Fire by George R.R. Martin has become wildly popular in the past few years. This popularity is in part due to the shocking twists and turns of the series, the cultish catchphrases and some impressive characterisations. This report aims to provide insights about Games of throne characters and the story line using twitter data. The report is useful for production house of Game of thrones and the team. Using pyspark and twitter, social tweet data across the seasons has been analysed. People reaction and popularity has been visually presented. The most popular deaths across all seasons, death of character is popular episode, interest peak across seasons, most liked and hated charcater, popular house in each season, popular charcaters across seasons, most shocking seasons and the shocking episodes alonngwith a sentiment analysis of each season in Game of Thrones is attached in this report.
Business Overview
Considering the cult like status and overarching popularity enjoyed by the series - Game of Thrones, it is only natural that production houses interested in creating and funding more such shows would need to study the trends of GOT. By reviewing the most popular worldwide opinion on Game of Thrones, they can focus on what worked for the show and try to replicate it on other shows to garner attention from the viewers.
While multiple sources of reviews for the shows from IMDB, rotten tomatoes, etc… are present, none provides a virtual window to a viewer opinion as twitter. Live tweeting allows users to communicate and share their joy, horror and sorrows as it happens on their TV. Thus this paper would use twitter data to analyse the sentiment across seasons and the popularity on certain factors. It shall focus on which most liked character, and the most hated ones. It will show the times of peak interest, the points of interest and the most talked about seasons and characters, etc….All these factors may help production house to create specific scenarios and characters that might induce tremendous attention on the show – be it positive or negative.
Datasets
Games of Thrones Character data has been extracted from Game of Thrones Wikia. A kaggle dataset on Game of Thrones characters was extracted in the csv format anda placed in HDFS. This file was used to identify the overall deaths in the show. All csv files were placed in HDFS.
Twitter Data Extraction
Python program is used to extract twitter data for all seasons (2011 till 2016). Tweets are extracted for particular periods of time when the episodes were aired. Google trends was initially used to identify the periods of maximum discussion on the specific topic. From the trends data, the most active months were identified and twitter data was extracted for those periods of time. 
Using pyspark - SparkContext and SQLContext, data was extracted in RDDs. Data in RDD was then cleaned using regular expression pattern. 

Business Questions Identified
1.	Most stunning deaths discussed across all seasons.
2.	Most stunning deaths by episode.
3.	Character Death by House Alignment
4.	Most Popular Hashtags across Seasons
5.	Sentiment analysis across seasons. (Positive and negative )
6.	Most Liked character across seasons.
7.	Most Hated character across seasons.
8.	Most popular house in Game of Thrones.
9.	Most Shocking Season in Game of Thrones.
10.	Most Shocking episodes across seasons.
Methodology and Tools:
Methodology:
Top Stunning Deaths:
•	Death synonyms are identified using Thesaurus and list is created. Using SQlContext tweet data is filtered based on the keywords identified for death. Dataframe with all the tweets where death is discussed will be created.
•	RDD is created to read all the characters and related data extracted from wikia. The dead character names are separated from RDD and a list is created. 
•	Function compare is created to search for character name in tweet and return the character name found in the tweets which are related to death.
•	RDD is looped through each element (tuple) and the compare function is called sending individual tweet and the list of dead character names. This RDD is converted to Dataframe by defining schema and using SQLContext. This Dataframe is registered as Table. 
•	The table is queried to get the Dead Character Names discussed in tweets, Season number and number of times the character death is discussed in that season.
•	Result is pushed to csv file (File Name : PopularDeaths.csv)

Top Popular Characters:

•	Twitter data in csv is extracted using RDD.
•	Another RDD is created to read all the characters and related data extracted from wikia..Function compare is created to search for character name in tweet and return the character name found in the tweets which are related to death.
•	RDD is looped through each element (tuple) and the compare function is called sending individual tweet and the list of character names. This RDD is converted to Dataframe by defining schema and using SQLContext. This Dataframe is registered as Table. 
•	The table is queried to get the popular  Character Names discussed in tweets, Season number and number of times the character is discussed in that season.
•	Result is pushed to csv file (File Name : PopularCharacters.csv)
Sentiment Analysis:
Textblob python package is imported . This package is used to process textual data. This provides API  which used Natural language processing to do part of speech tagging, sentiment analysis	, ngrams and other text analytics tasks.
•	Tweets extracted in RDD are cleaned all additional white spaces,hash tags, user mentions, html tags, url links and other special characters are removed using regex pattern and sub
•	Once the tweets are cleaned each tweet is sent to textblob and the sentiment polarity of that tweet is processed. 
•	RDD is converted to dataframe using  defined schema and queries to get each day tweet aggregated polarity ,tweet date and characters mentioned in that tweet
•	Negative polarity is identified as negative sentiment and positive polarity as positive sentiment. Zero polarity signifies neutral polarity
•	Result is pushed to csv file (File Name : Sentiment.csv)

Interest Peak:

•	Tweets extracted in RDD is converted to dataframe and registered as table.
•	The table is queries to get Daily tweet counts and season information
•	Result is pushed to csv file (File Name : InterestPeak.csv)


Shocking Seasons and episodes:
•	Shocking synonyms are identified using Thesaurus and list is created. Using SQlContext tweet data is filtered based on the keywords identified for shock words. Dataframe with all the tweets where shock isexpressed
•	RDD is created to read all the season and episode information from wikia. 
•	Function compare is created to search words which express shock in tweets and return the word found in the tweets.
•	RDD is looped through each element (tuple) and the compare function is called sending individual tweet and the list of shock words. This RDD is converted to Dataframe by defining schema and using SQLContext. This Dataframe is registered as Table. 
•	The table is queried to get the tweet date and number of times shock is expressed in tweets
•	Result is pushed to csv file (File Name : ShockingFactor.csv)

Tools Used:
Python program is used to extract the twitter data across all seasons and the specific month when the seasons are aired. Character details and episode details are scrapped from Wikia.
Pyspark core and SQLContext is used to clean and process data. Once the processing is done resultant data used for visualization is pushed to csv file (summarised data).
Visualization is done using Tabluea using the CSV files generated from pyspark.

Analysis and Results:
Interest Peaks
The trends above clearly indicate the attention that has been paid to the show across the years. As can be seen, the interest in the first few seasons peaked once the show started airing in April. However, in the last year, it is evident that the interest has started picking up even in the initial months before the start of the season. This is a powerful indicator of how sustained interest has been generated over the course of the years.
 
Character Death by House Alignment
The GOTDeathCharacter.csv consists of list of characters and their house information to which they are aligned. This is an original dataset with actual data from the show. We will be using this file to get a basic rundown of the massacre. We will be doing the comparison between houses.
•	Loaded the dataset(GOTCharacterDeath.csv) file into HDFS
•	Created a spark program and stored the file contents in ‘Dead’ variable.
•	Extracting the relevant columns from the file i.e Allegiance column and GOTCharacter. Storing the content in another variable –‘Deadchar’.
•	Grouping the count by allegiance and adding the sum of all GOTcharacters dead belonging to the same house.
•	Using the reduceByKey function to get the count and storing the results in ‘countdead’ variable. 
•	Converting the RDD to dataframe and then writing it to a csv file. 

 

Things are starting to look up. The House Targaryen, do seem to have lost the most named characters tied to their house followed by House Stark and Lannister. 
Most stunning deaths discussed across all seasons
Jon Snow’s death in season 5 amassed the highest tweets closely followed by the death of Joffrey Baratheon. While the death of Jon Snow was highly talked about, the death of Joffrey Baratheon was also a totally unexpected twist which created a huge reaction from the audience. 
 
 
Most stunning deaths by episode 
The most stunning deaths by episode is plotted . The episode “The lion and the rose with the death of Jeoffry Baratheon tallies high against almost all other episodes. However, it is also clear that some of the data that should have been captured is not showing up well here. For example, the death of Robb stark in the Rains of Castamere was a huge shock in the series which is not adequately represented here. Same is the case with the season 1 episode Baelor in which Ned Stark is executed. 


 
Sentiment analysis across seasons. (Positive and negative)
Sentiment across the episodes for each season is plotted. Peak interest and high volumes of sentiment are expressed in season 3 which had the terribly shocking Red Wedding. The popularity of game of thrones series is clearly showing up in the positive sentiments expressed by viewing audiences across twitter.
 

 
Most Liked character across seasons
Jon Snow ranks high in the most liked character across seasons. This along with the most stunning deaths data indicates the popularity and the strength of the character portrayal. This indicative measure can be used to model a series’ popular characters.
 

 
Most popular House in Game of Thrones
The popular houses across seasons provides interesting data trends. House Bolton, that was almost non-existent in the first couple of seasons shot up in popularity in season 3 – However, this should be interpreted as “unpopularity” since the sentiment here would be mostly negative since they were the directors of the Red wedding. 
House Stark and the night watch shows a steady increase in popularity collaborating the popularity of Jon Snow.
 

 
Most Shocking Season in Game of Thrones
The most shocking season seems to be season 3 and season 5 where two of the major turn overs in the story happens – Red Wedding and the death of Jon Snow. This proves that even wildly perceived unpopular turns in the story induce audience interest.
 

Most Shocking episodes across seasons
 

Wrapping up the Fantasy:
From the twitter trends, the following can be observed –
•	Shock value in shows seems to make an impact on the viewing audience. Higher the shock value, greater the popularity or unpopularity of the show.
•	GoT trends show that sustained interest can be built up over time by providing strong characterizations.
•	Sentiment analysis allows us to decipher the general mood of the audience. It must be noted that a positive sentiment on the show is important for the success of the show.
•	Audiences identify and root for certain characters in the show. This popularity can be tapped to deliver successful entertainment to the public.
