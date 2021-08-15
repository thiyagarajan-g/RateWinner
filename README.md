# RateWinner
Winner Rating  Repo

This is based on Spark 3.2 on Scala 2.12 maven project.

This code base generates the random user ratings for popularity and polarity. It collects all the ratings for all the candidates and generates the Winner Rating metric and publishes them.

This is a vanilla code base and can be expanded as desired.

Please set up local spark to run the application.

Alternatively you can run the application on IntelliJ or Eclipse as a maven project.

Pre-requisites applications to get Installed:-
1) JDK 1.8 
2) Git client
3) Maven 
4) Spark Hadoop Version 3.2
5) Scala 2.12
6) Works only on macos / linux ( Windows, you can run within IntelliJ / Eclipse )

Steps to Run the Program:-

1) Install git client
2) Install Maven
3) Download Spark 3.2 and untar in a folder, we call that folder - Spark Home
4) Clone the git repo
5) Go to the repo
6) Run - mvn clean package - you will see maven installing all the libraries and compiling the sources
7) go to the Spark Home folder
8) Run the command - ./bin/spark-submit --class org.thi.RateWinnerMain /Users/thi/works/RateWinner/target/RateWinner-1.0-SNAPSHOT.jar
9) You will see the program generating the Random Popularity and Polarity Ratings and generating the Winner Rating Metric for all the candidates. We declare the candidate with highest Winner Rate Metric.





