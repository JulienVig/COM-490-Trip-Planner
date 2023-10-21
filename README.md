# Robust Journey Planning

| |Overview  | 
| - | - |
|Course| COM-490 Large-scale data science for real-world data |
| Year | Spring 2022 |
| Final Grade | **6 / 6** |
| Languages | Python, Spark, Hive, HDFS |
| Team size | 4 |

In the context of the course project, we implemented a journey planner which received the maximal grade. The aim of the project was to leverage the Swiss public transport data to implement a public transit journey planner in the area of Zurich. Notably, our trip planner assesses the success rate of each connection, letting the user choose the connection confidence level of his or her journey.
For that purpose, we implemented and adapted the [RAPTOR algorithm](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf), developed by Microsoft and used by OpenStreetPlanner, to take into account the success probabilites, by measuring transit delay frequencies at each station and measuring connection success probabilities.
The data, stored as a Hive table, was processed using PySpark and the algorithm was implemented in Python.

While the purpose of the project is to implement tackle a large scale algorithmic problem, we also implemented a simple user interface to showcase our tool.

https://github.com/JulienVig/COM-490-Trip-Planner/assets/33122365/58f28949-b3a2-4495-936f-3edec518d58f



