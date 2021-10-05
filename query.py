import csv, os, sys, sqlite3
conn = sqlite3.connect('upwork.db')

cur = conn.cursor()

#Note that SQLITE queries using the LIKE clause are case insensitive.
queries = [
    "JSS"
]

done = []
for query in queries:
    with open (f'{query} Query Results.csv', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(["Thread Title", "URL", "Post Author", "Post Author Rank" "Post Date", "Post Text", "Edit Status"])
        cur.execute(f"SELECT thread_title, thread_url, tid FROM threads WHERE thread_title LIKE ?", ('%' + query + '%', ))
        rows = cur.fetchall()
        print(f'{query}: {len(rows)} threads')
        for row in rows:
            thread_title = row[0]
            thread_url = row[1]
            tid = row[2]
            if tid not in done:
                cur.execute(f"SELECT content, author_id, post_date, edit_status FROM posts p WHERE p.tid=?", (tid, ))
                post_rows = cur.fetchall()
                r = []
                print(f'{thread_title}: {len(post_rows)} rows')
                count = 0
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
                    count += 1
                print(f'Wrote {count} posts')
                done.append(tid)
                print(f'Completed thread {thread_title}')
    print(f'Wrote {query} Query Results.csv')

