# Project Hierarchy

the project directory is demonstrated as below: 

![image](https://github.com/hecanyilmaz/scrapy-docker/assets/71831337/e83949e1-38ca-49a9-98c6-b0875d266c90)


## Note for running the project
The uploaded .zip file doesn't include data folder,
to see the results of the program as intended
please add data folder.


## Commands
After you go into the directory of the project you need to follow the steps:

1- docker build -t scrapy . -> to build scrapy image
2- docker compose up -> to orchestrate the whole project

At the end you are going to obtain output.csv file as wished right in the container

## Note for pipeline
I handled all attributes except "meta_data". I couldn't find out what causes the error for the meta_data project so I decided the disclude it.

* "benefits", "languages", "category", "tags", "tags5", "tags6" attributes consist of arrays. They needed to be changed to write INSERT query. I converted them by adding the "ARRAY" utility in front of the arrays. Or, if the array is empty I converted the values into NULL.
* For "description" attribute, I added single quotes to beginning and end of the value.
* For "categories" attribute, I converted them into array of strings from the array of dictionaries.
* For others I either let them as they are by looking up their type or added single quotes.
