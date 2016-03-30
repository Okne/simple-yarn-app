# simple-yarn-app
Description
------
Application master start single container, which: 
* read input file line by line
* retreive pageURL from line
* make http request to this pageURL
* parse response and retreive top 10 keywords
* add keywords to read line in format **keyword1, keyword2,...**
* write created line to output file
* request timeout errors and responses with 404 status are logged into stderr

To build and run application
------

* build jar with maven
* go to build dir
* copy app jar to hdfs with
```bash
hdfs dfs -copyFromLocal yarn-app-1.0-SNAPSHOT.jar <path-to-jar-on-hdfs>
```
* copy input file to hdfs with
```bash
hdfs dfs -copyFromLocal <path-to-input-file-on-local-machine> <path-to-input-file-on-hdfs>
```
* run application on yarn with jar
```bash
yarn jar yarn-app-1.0-SNAPSHOT.jar hdfs://<df.defaultFS>/<path-to-input-file-on-hdfs> hdfs://<df.defaultFS>/<path-to-output-file-on-hdfs> hdfs://<df.defaultFS>/<path-to-jar-on-hdfs> 
```
where **df.defaultFS** - value of **df.defaultFS** property from hdfs config core-site.xml
* copy output file from hdfs with -copyToLocal option of hdfs
