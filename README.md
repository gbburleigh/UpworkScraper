# UpworkScraper

Dataset of threads, posts, and users taken from Upwork's community forum, along with the scraper used to aggregate the data.

| Threads | 72,517  |
|---------|---------|
| Posts   | 456,626 |
| Users   | 73,830  |

<h1>Analyzing Data</h1>

Dataset is written to a sqlite database that can be queried directly for further analysis. Python's sqlite package offers a direct interface that we can leverage to pull specific queries with. 

<ul>
  <li>First, place upwork.db in the working directory. Import the sqlite3 package, and any desired data management packages (for this example, we'll write to .csv). Make a connection to the database after importing sqlite3.
    '''
    import sqlite3
    
    conn = sqlite3.connect('upwork.db')
    cur = conn.cursor()
    '''
  </li>
  <li><i>cur</i> allows us to make queries to the database. We'll start by fetching posts from threads with the title 'New to Upwork'.
  '''
  #Instantiate our writer and query
  query = 'New to Upwork'
  
  #Track fetched tids to avoid double counting
  done = []
  with open (f'{query} Query Results.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(["Thread Title", "URL", "Post Author", "Post Author Rank" "Post Date", "Post Text", "Edit Status"])
        
        #Format our query
        #query = ('%' + query + '%', )
        
        #Make our query
        cur.execute(f"SELECT title, thread_url, tid FROM threads WHERE title LIKE ?", (query, ))
  '''
  Note that the query can be formatted with additional '%' symbols to allow strings that aren't exactly the same but contain that text to be matched (i.e. "I'm   New to Upwork" would be a match in this example). See <a href=https://www.sqlitetutorial.net/sqlite-like/>here</a> for further documentation of the LIKE clause. Also note that formatting sqlite queries in Python can be unusual; putting in variables in strings must be done with the ? character, and referencing the given variables in a tuple. So in our <i>cur.execute</i> call, the first argument is our query string and the second argument is the tuple containing the variable to substitute (query, ). 
  </li>
  
  <li>After making the query, we can fetch the data from the cursor and put it in a variable. Depending on the SELECT statement you make, the data returned can be different as well as the order you choose to select them in.
    '''
    #Every thread we found that matches our query
    rows = cur.fetchall()
  
    #For each thread
    for row in rows:
        #Fetch the thread info so we can get it's posts
        thread_title = row[0]
        thread_url = row[1]
        tid = row[2]
    '''
</li>
<li>We'll apply the same techniques to fetch all of the posts and users for each thread.
  '''
  if tid not in done:
      cur.execute(f"SELECT message_text, author_id, post_date, edit_status FROM posts p WHERE p.tid=?", (tid, ))
      post_rows = cur.fetchall()
      for post in post_rows:
          text = post[0]
          aid = post[1]
          date = post[2]
          edit_status = post[3]
          cur.execute(f"SELECT user_name, user_rank FROM users WHERE uid=?", (aid, ))
          info = cur.fetchall()[0]
          author = info[0]
          rank = info[1]
          writer.writerow([thread_title, thread_url, author, rank, date, text, edit_status])
      done.append(tid)
  '''
</li>
 </ul>
 Other options include pandas or native Python dictionaries(not recommended) for data storage, which can be applied similarly to the csv writer in this example. 
 
 <h2>Database Schema</h2>
 
 In order to relate entries in each of the four tables (<it>threads</it>, <it>posts</it>, <it>users</it>, <it>categories</it>), the database uses a system of ids for all entries, hashed from the information stored with it. As shown above, obtaining the id of a given element can be used to find associated entries in other tables. You can search for posts or threads made by a given user id, look for all posts under a given thread id, or count the number of threads made in a given category. 

<h3>Additional Queries</h3>

Timestamps can be compared to tally posts falling in some range.

'''

SELECT COUNT(*) FROM posts p WHERE post_date > '2021-03-01' AND post_date < '2021-03-30';

SELECT COUNT(*) FROM posts p WHERE post_date > '2021-03-01' AND post_date < '2021-03-30' AND edit_status!='Unedited';

'''

