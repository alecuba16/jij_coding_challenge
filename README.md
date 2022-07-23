# Adevinta Spain Data Engineer Coding Challenge

Today and urgently, the Adevinta Spain CEO requested some vital information about published ads and sites at Adevinta Spain.

The Head of Data steps forward knowing that Adevinta Spain built a Data Lake and therefore the required information will be available in there.

There is only one problem: the Data Engineers are currently on a deserved long vacation and there is no one else left to accomplish this task.

The Chief Technology Officer (CTO) managed to extract the required data and after the approval from the Chief Information Security Officer (CISO) uploaded them in this link: [data set](https://www.kaggle.com/gumartinm/dataset).

Help the Head of Data to complete the task through the implementation of the next requirements.


## Exercise

### Tasks

* task_1 : Find out how many ads have been published between 2020-11-05 and 2020-11-07 (both included)
* task_2 : Calculate revenue of the ads. Being revenue = **adPrice** * **impressions**
* task_3 : Calculate average impression by siteName. For each site: (Σ**impressions**) / (count **adId**)


### Requirements

- Well structured, documented and maintainable code
- Unit tests to test the different components
- Errors handling
- Output of each task should be a CSV format (comma separator)


## Data set

Data is available in CSV files in this location: [data set](https://www.kaggle.com/gumartinm/dataset). It is compound of three tables, the diagram is shown down below:

![diagram](screenshots/diagram.png)


There is no need of running this application in a cluster, you should be able to do it in your local machine.



