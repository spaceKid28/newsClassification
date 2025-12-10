STEP 0: ssh into the cluster and activate virtual environment for python scripts
    ssh into cluster
    Navigate here:
        cd /home/hadoop/belincoln/newsClassification/sentiment_analysis/
    Activate the virtual environment
        source hackerNews/bin/activate
    If there is an error, you can rebuild the virtual environment using the requirements.txt file found here /home/hadoop/belincoln/newsClassification/sentiment_analysis/
    Note that this python libraries take up disk space in the local cluster filesystem
    (hackerNews) [hadoop@ip-172-31-81-29 sentiment_analysis]$ du -sh hackerNews
    15M     hackerNews

STEP 1 (SCRAPE FROM INTERNET):
In this step, we pull article text from the HackerNews api. HackerNews is a ycombinator site that provides news articles about the technology industry.
The webscraping files are already located in the cluster, simply naviage here: cd /home/hadoop/belincoln/newsClassification/sentiment_analysis
and run these commands. you can change --num-stories to a larger number for a true batch pull (1000 took about 10 minutes)
    
    python hackerNews_batch.py --num-stories 500 --output data/

This raw data is stored in the local filesystem, we must use it to hdfs using a put command (this command is relative so make sure you are in /home/hadoop/belincoln/newsClassification/sentiment_analysis/ )
    hdfs dfs -put ./data/*.jsonl /inputs/belincoln_hackerNews/raw/

delete the files out of local filesystem as other students are using it
    rm -r ./data/*.jsonl

Make a note of the number of files in hadoop: (I could run the batch job overnight and get a lot more)
    (hackerNews) [hadoop@ip-172-31-81-29 sentiment_analysis]$ hdfs dfs -du -h /inputs/belincoln_hackerNews/raw/
    1.2 M    2.5 M    /inputs/belincoln_hackerNews/raw/hn_batch_101_200.jsonl
    1.5 M    2.9 M    /inputs/belincoln_hackerNews/raw/hn_batch_1_100.jsonl
    1.1 M    2.2 M    /inputs/belincoln_hackerNews/raw/hn_batch_201_300.jsonl
    1.5 M    3.0 M    /inputs/belincoln_hackerNews/raw/hn_batch_301_400.jsonl
    292.5 K  585.1 K  /inputs/belincoln_hackerNews/raw/hn_batch_401_427.jsonl


STEP 2 RUN FASTAPI FOR ML INFERENCE
After ssh-ing into our hadoop cluster

Must deploy a fastapi app in order to run classification

After activating the virtual environment, you can launch the app used to perform the machine learning classification using the below command. I ran it on port 3003, which is the same port I used for my webapp (the webapp is running on a different cluster).
    python -m uvicorn app:app --host 0.0.0.0 --port 3003

Note that when this app is spun up, it downloads the model from huggingface and saves it on the hadoop cluster (in the local filesystem, not hdfs)
I tested a couple of models locally, and found that this model was a good balance between performance and size. It takes up 1.6G on the cluster, which is not trivial as it is not in hdfs

        (hackerNews) [hadoop@ip-172-31-81-29 scala-2.12]$ ls -lh ~/.cache/huggingface/hub/
        total 0
        drwxr-xr-x. 6 hadoop hadoop 65 Dec 10 02:37 models--facebook--bart-large-mnli
        (hackerNews) [hadoop@ip-172-31-81-29 sentiment_analysis]$ du -sh ~/.cache/huggingface/hub/
        1.6G    /home/hadoop/.cache/huggingface/hub/

I probably should have sent an email about this usage (if all students did this, it would make the cluster unusable). I was in a bit of time crunch but made sure that
there was enough memory on the cluster

[hadoop@ip-172-31-81-29 belincoln]$ free -h
total        used        free      shared  buff/cache   available
Mem:           247Gi        19Gi       198Gi       9.0Mi        29Gi       225Gi
Swap:             0B          0B          0B
[hadoop@ip-172-31-81-29 belincoln]$


STEP 3: BATCH LAYER UPDATE
ON MY LOCAL MACHINE (LAPTOP)
My Batch Layer is a scala program and is written using BatchClassifier.scala
Compile the code into a jar file using:
    sbt clean assembly
Then I upload the .jar file to the hadoop cluster at this location: /home/hadoop/belincoln/newsClassification/sentiment_analysis/scala_classifier/target/scala-2.12

and ran using spark-submit
    spark-submit   --master yarn   --deploy-mode client   --driver-memory 512m   --executor-memory 512m   --num-executors 1   --executor-cores 1   --conf spark.dynamicAllocation.enabled=true   --conf spark.dynamicAllocation.minExecutors=1   --conf spark.dynamicAllocation.maxExecutors=4   --class BatchClassifier   batch-classifier.jar   hdfs:///inputs/belincoln_hackerNews/raw/   http://ip-172-31-81-29.ec2.internal:3003/classify

For the command to work, the cleaned data must be in hdfs:///inputs/belincoln_hackerNews/raw/ and the app in app.py must be running on the correct port (3003)

The scala program writes to hive using the ORC format (here is the code snippet in BatchClassifier.scala)
    classified.write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("belincoln_hackernews_classified")

You can test if everything has been done correctly: select * from belincoln_hackernews_classified limit 1;

To see how many articles in the batch layer have a positive vs negative sentiment towards ai, you can run this command
in beeline
start beeline session
    beeline -u 'jdbc:hive2://localhost:10000/default' -n hadoop
0: jdbc:hive2://localhost:10000/default> SELECT
. . . . . . . . . . . . . . . . . . . .>     sentiment_label,
. . . . . . . . . . . . . . . . . . . .>     COUNT(*) as article_count,
. . . . . . . . . . . . . . . . . . . .>     AVG(sentiment_score) as avg_confidence,
. . . . . . . . . . . . . . . . . . . .>     AVG(score) as avg_hn_score,
. . . . . . . . . . . . . . . . . . . .>     AVG(descendants) as avg_comments,
. . . . . . . . . . . . . . . . . . . .>     SUM(score) as total_hn_score
. . . . . . . . . . . . . . . . . . . .> FROM belincoln_hackernews_classified
. . . . . . . . . . . . . . . . . . . .> WHERE sentiment_label IN ('optimistic', 'pessimistic')
. . . . . . . . . . . . . . . . . . . .> GROUP BY sentiment_label;


Here is my output:
+------------------+----------------+---------------------+---------------------+---------------------+-----------------+
| sentiment_label  | article_count  |   avg_confidence    |    avg_hn_score     |    avg_comments     | total_hn_score  |
+------------------+----------------+---------------------+---------------------+---------------------+-----------------+
| pessimistic      | 230            | 0.5891591486723526  | 166.51739130434783  | 106.74782608695652  | 38299           |
| optimistic       | 186            | 0.5811017014647043  | 204.71505376344086  | 116.70430107526882  | 38077           |
+------------------+----------------+---------------------+---------------------+---------------------+-----------------+

avg_confidence is how confident the model is when making the classification. (59% is reasonably good, there are a lot of articles in there not about ai, so those will bring
confidence down closer to 50%. The avg_hn_score is just the number of "up votes" an article gets (works kind of like reddit). 


In our download from the hackernews api, our python script will only write an article if it can successfully pull the text. In this case, 
only 416 articles were successfully pulled. We ran our machine learning model (bart) on all of them, and 230 were considered pessimistic about the future of ai, 
while 186 were optimistic about the future of ai. 

STEP 4, SERVING LAYER / WRITE TO HBASE
STEP4A Create Analytics Table
Now we create the tables in hbase we will use in our web app
We build a table that gives us the number of articles per day, the average number of upvotes and comments for each article. This gives us an idea of how active
hacker news is on that day. 

This table is called belincoln_daily_hackernews and can be found in "serving_layer_tables.hql"

I then created the hbase table, first I created the table in hbase shell (column type is 'metrics')
    hbase shell
    
    create 'belincoln_daily_hackernews', 'metrics'

I then created the table in beeline using "create_hbase.hql" file

To verify the existence of our table in hbase, I used this in hbase shell and got the desired response

    scan 'belincoln_daily_hackernews', {LIMIT => 5}
hbase:004:0> scan 'belincoln_daily_hackernews', {LIMIT => 5}
ROW                                         COLUMN+CELL                                                                                                                 
2025-11-27                                 column=metrics:article_count, timestamp=2025-12-10T07:59:11.048, value=\x00\x00\x00\x00\x00\x00\x00\x02                     
2025-11-27                                 column=metrics:avg_comments, timestamp=2025-12-10T07:59:11.048, value=84.5                                                  
...

STEP 4B CREATE Machine Learning Classification Table

Create the belincoln_daily_hackernews_discrete table using beeline and the hql  code in serving_layer_tables.hql
After the hive table is created, start an hbase shell  and follow the same code in create_hbase to create the hbase table. 
    hbase shell
    create 'belincoln_daily_hackernews_discrete', 'metrics'

Then run the code in create_discrete_hbase.hql (in beeline or with a command). 
Make sure the table exists
# In hbase shell
scan 'belincoln_daily_hackernews_discrete', { FILTER => "SingleColumnValueFilter('metrics','day',=,'binary:2025-12-05')", COLUMNS => ['metrics:title','metrics:author','metrics:upvotes','metrics:url'], LIMIT => 3 }

    hbase:017:0> scan 'belincoln_daily_hackernews_discrete', { FILTER => "SingleColumnValueFilter('metrics','day',=,'binary:2025-12-05')", COLUMNS => ['metrics:title','metrics:author','metrics:upvotes','metrics:url'], LIMIT => 3 }
    ROW                                         COLUMN+CELL                                                                                                                 
    http://blog.modelcontextprotocol.io/posts/ column=metrics:author, timestamp=2025-12-10T21:53:00.041, value=2025-12-09                                                  
    2025-12-09-mcp-joins-agentic-ai-foundation                                                                                                                             
    /                                                                                                                                                                      
    http://blog.modelcontextprotocol.io/posts/ column=metrics:title, timestamp=2025-12-10T21:53:00.041, value=arthurdenture                                                
    2025-12-09-mcp-joins-agentic-ai-foundation                                                                                                                             
    /                                                                                                                                                                      
    http://blog.modelcontextprotocol.io/posts/ column=metrics:url, timestamp=2025-12-10T21:53:00.041, value=0.8062434792518616                                             
    2025-12-09-mcp-joins-agentic-ai-foundation                                                                                                                             
    /                                                                                                                                                                      
    http://muratbuffalo.blogspot.com/2025/12/o column=metrics:author, timestamp=2025-12-10T21:53:00.041, value=2025-12-02                                                  
    ptimize-for-momentum.html                                                                                                                                              
    http://muratbuffalo.blogspot.com/2025/12/o column=metrics:title, timestamp=2025-12-10T21:53:00.041, value=zdw                                                          
    ptimize-for-momentum.html                                                                                                                                              
    http://muratbuffalo.blogspot.com/2025/12/o column=metrics:url, timestamp=2025-12-10T21:53:00.041, value=0.5937113165855408                                             
    ptimize-for-momentum.html                                                                                                                                              
    http://oldvcr.blogspot.com/2025/12/oblast- column=metrics:author, timestamp=2025-12-10T21:53:00.041, value=2025-12-07                                                  
    better-blasto-game-for-commodore.html                                                                                                                                  
    http://oldvcr.blogspot.com/2025/12/oblast- column=metrics:title, timestamp=2025-12-10T21:53:00.041, value=todsacerdoti                                                 
    better-blasto-game-for-commodore.html                                                                                                                                  
    http://oldvcr.blogspot.com/2025/12/oblast- column=metrics:url, timestamp=2025-12-10T21:53:00.041, value=0.5707244277000427                                             
    better-blasto-game-for-commodore.html                                                                                                                                  
    3 row(s)
    Took 0.0083 seconds                                                                                                                                                     
    hbase:018:0>








Step 5: Web Application

Upload the articles_web_app to the webapp cluster (I've already done this, this folder is under belincoln)
After navigative to /home/ec2-user/belincoln/articles_web_app Run 
    npm install (I've already run this )
    node app.js 3003 http://ec2-54-89-237-222.compute-1.amazonaws.com:8070

You can access the web app at this url: node app.js 3003 http://ec2-54-89-237-222.compute-1.amazonaws.com:8070


Possible improvements:

After I perform the ml inference call, I actually don't need the text. I have the text saved in two places, in hadoop in a .jsonl format
and in hive in a column called 'text' in belincoln_hackernews_classified. There's no reason to write all of that text to hive. 
Originally, I thought I may do something in the serving layer, like have my web app be able to allow users to select a title and then see the article...
But I did not do that. Also, because I have saved the URL to hive, that doesn't really make sense from a cost perspective, to pay for storage for a bunch of text.
It is only needed when I run the batch llm inference call. Good idea to keep it in hadoop, so we have a single source of truth.

I don't know much about our cluster configuration or about GPU vs CPU provisioning, but the ML part of project is clearly running on a CPU, 
so I should have put more thought into that. But the spark job ran in about 20 mins, after I got allocated through YARN, on 415 articles, so I thought that was reasonably fast all things considered. 
When I looked at the logs it seemed that taking the POST request to my app was taking a long time. As opposed to the work we did in class with map-reduce, it seems
that my spark job was not highly parallelizable. I'm sure there's a way to refactor my code in BatchClassifier.scala to be more performant, I should spend more time looking at that code.

My web scraper is bad and the text parsing is worse, but I wanted to spend more time on other parts of the project.
I could spend more time building more and better webscrapers that pull other types of text (New York Times, tweets from twitter, ect..)
If i was willing to pay for the api keys. I wanted to only ask the question "is this article optimistic or pessimestic about AI", 
and most articles from other sources aren't talking about ai. However, most articles from hackernews are talking about ai, and
there is no key required to pull their text, so it made sense. 

Also the most glaring ommission from my project is a lack of speed layer. In fact, I could have just queried the hive tables instead of
hbase in my serving layer. I thought I was going to have time to do the speed layer and update hbase tables. Specifically, the table
belincoln_daily_hackernews doesn't utilize any of the classification data (the only fields are  date, article_count, avg_upvotes, avg_comments,
which all come directly from hackernews api (and that was by design, I intended to create a serving layer that updated that table (increment article counts 
like we incremented delays in class). I'm not sure about the other table, I didn't want to try and make a call to my fastapi app for the ml classification,
But I ran out of time. Fighting with the classification algorithm took me a lot of time. Something I may be able to look into later!


