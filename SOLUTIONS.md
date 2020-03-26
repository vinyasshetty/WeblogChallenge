I have gone through 2 iterations while solving this problem.
I have used Spark/Scala to implement the requirements.

I have uploaded the scala file(with comments) and also the Zepellin Json File which i
used for analyzing the data.
Zepellin File has the sample results also.

**My Final Solution would be Approach 3 Code : com.viny.approach3.WebLogMain.scala**  
Zepellin : approach3.json

Approach 1: 
In this approach I used a window size of 15 mins, but the window start and end time are fixed(predetermined) for all users
ie If sample data looks like below:
```
123,2015-07-22 02:40:00,google.com
123,2015-07-22 02:44:00,bing.com
123,2015-07-22 02:53:00,yahoo.com
999,2015-07-22 02:38:00,paytm.com
999,2015-07-22 02:50:00,nytimes.com

my sample approach 1 window range will look like this :

[2015-07-22 02:30:00, 2015-07-22 02:45:00],123,google.com
[2015-07-22 02:30:00, 2015-07-22 02:45:00],123,bing.com
[2015-07-22 02:30:00, 2015-07-22 02:45:00],999,paytm.com
[2015-07-22 02:45:00, 2015-07-22 03:00:00],123,yahoo.com
[2015-07-22 02:45:00, 2015-07-22 03:00:00],999,nytimes.com
```
As you see, all the user session time and url info is tracked within that predetermined window start and end time .

Once this grouping was done , I implemented the 4 requirements in the code package:
com.viny.approach1.WebLogMain

## Final Solution Approach 3:  
I realized using same fixed start and end time window session for all users does NOT
give the personalization per user, I would ideally want the 15 minute window to start for a user 
when he first appears and keep him in the same session for next 15 mins and once 15 mins is up, then 
a new session should start for the same user when he reappears again.

Now the challenge here was to create different "session id" for each clients based on window time.
So to create the session id's ,I used a UDAF along with a window function. Implementation details 
are given in the Code:
com.viny.approach3.WebLogMain.scala

Faced issue with timestamp UTC data, where spark was converting it to my system clock by default
when I casted it to timestamp, so had to handle that explicitly.I wanted to keep the time in UTC. 
But even with converted PST time , it would not have impacted my session id generation.

Also initially when i was calculating the time diff as a part of SessionId calculation, 
i converted the timestamp to long but realized that when i did that it took information 
only upto seconds from timestamp, but thought of retaining the entire information till
microseconds and amd changes to the datatype accordingly.
