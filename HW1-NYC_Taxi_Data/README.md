## Environment : 
Jupyter notebook : python 3.7.3

## Data : 
- Taxi data
    - Yellow Taxi Trip Records - 2019 : January ~ December
    - Data size : 240,000 ( 20,000 data per month which was random selected from each month - Yellow Taxi Trip Records )
- Zone data
    - **Latitude** and **longitude** of each LocationID Borough of each LocationID

## Q1 : What are the most pickups and drop offs region ? 

- Step 1 : 
We get **pickup DataFrame** and **drop-offs DataFrame** from Taxi data and Zone data.

- Step 2 : 
Replace LocationID which is not in range ( 1, 265 ) with NaN, and **remove missing values** from pickup DataFrame and drop-offs DataFrame.

- Step 3 : 
Use **K-means** and **DBSCAN** algorithm to analyze pickup DataFrame and drop-offs DataFrame, then plot scatter chart.

![](https://i.imgur.com/akqcepR.png)
![](https://i.imgur.com/naZhBTI.png)

- Step 4 : 
Compare to **Borough of New York City**
, we conclude  that the **Manhattan is the most pickups and drop offs region**.

![](https://i.imgur.com/WBFThGY.png)

## Q2 : When are the peak hours and off-peak hours for taking taxi ?

- Step 1 : 
Transform tpep_pickup_datetime and tpep_dropoff_datetime to the **hours format**.

- Step 2 : 
Plot the bar chart by the data, we can conclude that the **peak hours are 17 : 00 ~ 19 : 00** and the **off-peak hours are 3 : 00 ~ 5 : 00**.

![](https://i.imgur.com/YayJZTg.png)


## Q3 : 	What are the differences between short and long distance trips of taking taxi ?

- Step 1 : 
Calculate the **quartile** of trip distance, and plot the box chart.
    - Q1 is : 0.98 
    - Q2 is : 1.63 
    - Q3 is : 3.05
    
![](https://i.imgur.com/1m6henl.png)

- Step 2 : 
Define the **long distance as greater than equal to Q3** ( 3.05 ), **short distance as less than equal to Q1** ( 0.98 ).


- Step 3 : 
Plot bar chart of **long distance with borough** and **short distance with borough**.

![](https://i.imgur.com/y5jpYxF.png)

- Step 4 : 
We can find that **the short distance is much less than long distance** in borough : **Queens**.

- Step 5 : 
Plot bar chart of **long distance and short distance with pickup times and drop offs times**.

![](https://i.imgur.com/oLOwuU6.png)
![](https://i.imgur.com/wyQXJyn.png)


- Step 6 : 
We can find that the short distance and long distance are directly proportional to peak hours.
