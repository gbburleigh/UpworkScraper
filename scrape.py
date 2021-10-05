import time, json, csv, sys, os
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import sqlite3
from sqlite3 import Error
import hashlib

try:
    conn = sqlite3.connect(os.getcwd() + '/upwork.db')
except Error as e:
    print(e)

curs = conn.cursor()
def create_db():
    commands = ['''CREATE TABLE threads (
                        tid text NOT NULL,
                        category_name text NOT NULL,
                        thread_url text NOT NULL,
                        thread_title text NOT NULL,
                        thread_postdate timestamp NOT NULL,
                        thread_author_id text NOT NULL,
                        post_count text
                    );''',
                    '''CREATE TABLE posts (
                        tid text NOT NULL,
                        content text NOT NULL,
                        post_date timestamp NOT NULL,
                        edit_date timestamp,
                        author_id text NOT NULL,
                        editor_id text,
                        edit_status text NOT NULL,
                        post_page integer NOT NULL,
                        post_index integer NOT NULL
                    );''',
                    '''
                    CREATE TABLE users (
                        uid text NOT NULL,
                        user_name text NOT NULL,
                        user_url text NOT NULL,
                        join_date text,
                        user_rank text NOT NULL
                    );''']
    for command in commands:
        curs.execute(command)
try:
    create_db()
except:
    pass

targets = ['https://community.upwork.com/t5/Announcements/bd-p/news', \
                            'https://community.upwork.com/t5/Freelancers/bd-p/freelancers', \
                        'https://community.upwork.com/t5/Clients/bd-p/clients', \
                        'https://community.upwork.com/t5/Agencies/bd-p/Agencies',
                        'https://community.upwork.com/t5/New-to-Upwork/bd-p/New_to_Upwork',
                        'https://community.upwork.com/t5/Coffee-Break/bd-p/break_room']

skipped = ['https://community.upwork.com/t5/Announcements/Welcome-to-the-Upwork-Community/td-p/1',\
                        'https://community.upwork.com//t5/Announcements/Upwork-Community-Guidelines/td-p/3']

def insert_thread(thread_id, category_name, thread_url, thread_title, thread_postdate, thread_author_id, pages, post_count):
    curs.execute("SELECT rowid FROM threads WHERE tid = ?", (str(thread_id),))
    data = curs.fetchall()
    if len(data)==0:
        print(f'Inserting thread into database w/ id {thread_id}')
        curs.execute(f"""
            INSERT INTO threads(tid, category_name, thread_url, thread_title, thread_postdate, thread_author_id, post_count) VALUES
            (?, ?, ?, ?, ?, ?, ?);
        """, (str(thread_id), category_name, thread_url, thread_title, thread_postdate, 
            str(thread_author_id), int(post_count)))
        conn.commit()
    else:
        print('Thread id already found in DB')

def insert_post(thread_id, content, post_date, edit_date, author_id, editor_id, edit_status, post_page, post_index):
    data = curs.fetchall()
    curs.execute(f"""
        INSERT INTO posts(tid, content, post_date, edit_date, author_id, editor_id, edit_status, post_page, post_index) VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (thread_id, content, post_date, edit_date, author_id, editor_id, \
        edit_status, int(post_page), int(post_index)))
    conn.commit()

def insert_from_user(uid, user_name, user_url, join_date, user_rank):
    curs.execute("SELECT rowid FROM users WHERE uid = ?", (str(uid),))
    data = curs.fetchall()
    if len(data)==0:
        print(f'Inserting user into database w/ id {uid}')
        curs.execute(f"""
            INSERT INTO users(uid, user_name, user_url, join_date, user_rank) VALUES
            (?, ?, ?, ?, ?);
        """, (uid, user_name, user_url, join_date, user_rank))
        conn.commit()

def generate_next(url, _iter):
    return url + f'/page/{_iter}'

def parse_page(tar):
    curs.execute('SELECT thread_url FROM threads ORDER BY tid DESC LIMIT 1;')
    try:
        with open(f"{tar.split('/t5/')[1].split('/')[0]}_last_page.txt", 'r') as fp:
            last_page = fp.read()
            last_page = int(last_page)
    except:
        last_page = 1
    rows = curs.fetchall()
    last_url = rows[0]
    r = requests.get(tar)
    max_page_scroll = get_category_page_numbers(r.text)
    for currentpage in range(last_page, max_page_scroll + 1):
        if currentpage == 1:
            r = requests.get(tar)
        else:
            r = requests.get(generate_next(tar, currentpage))
        soup = BeautifulSoup(r.text, 'lxml')
        urls = get_links(soup)
        if last_url in urls:
            try:
                os.remove(f"{tar.split('/t5/')[1].split('/')[0]}_last_page.txt")
            except:
                pass
            with open(f"{tar.split('/t5/')[1].split('/')[0]}_last_page.txt", 'w') as fp:
                fp.write(currentpage)
        for url in urls:
            if url in skipped or len(curs.execute('SELECT * FROM threads WHERE thread_url = ?', (url, )).fetchall()) != 0:
                continue
            else:
                r = requests.get(url)
                parse(r.text, url, tar.split('/t5/')[1].split('/')[0])

def get_category_page_numbers(html):
    soup = BeautifulSoup(html, 'html.parser')
    menubar = soup.find('div', class_='lia-menu-bar lia-menu-bar-top lia-component-menu-bar')
    if menubar is not None:
        last = menubar.find('li', class_='lia-paging-page-last')
        try:
            pages = int(last.find('a').text)
        except:
            pages = int(last.find('span').text)
    else:
        pages = 1
    return pages

def get_thread_page_numbers(soup):
    menubar = soup.find('div', class_='lia-paging-full-wrapper lia-paging-pager lia-paging-full-left-position lia-component-menu-bar')
    if menubar is not None:
        last = menubar.find('li', class_='lia-paging-page-last')
        try:
            pages = int(last.find('a').text)
        except:
            pages = int(last.find('span').text)
    else:
        pages = 1
    return pages

def get_links(soup):
    hist = []
    urls = []
    for elem in soup.find_all('a', class_='page-link lia-link-navigation lia-custom-event', href=True):
        res = 'https://community.upwork.com/' + str(elem['href'])
        if res not in hist:
            hist.append(res)
            urls.append(res)
        else:
            continue
    return urls

def parse(html, url, categ, page_expire_limit=10):
    soup = BeautifulSoup(html.encode('utf-8').strip(), 'lxml')
    if soup is None or url in skipped:
        print(url)
    else:
        try:
            title = soup.find('h1', class_='lia-message-subject-banner lia-component-forums-widget-message-subject-banner')\
                .text.replace('\n\t', '').replace('\n', '').replace('\u00a0', '')
        except:
            title = url.split(categ + '/')[1].split('/td-p')[0].replace('-', ' ')
        try:
            post_date = soup.find('span', class_='DateTime lia-message-posted-on lia-component-common-widget-date')\
            .find('span', class_='message_post_text').text
        except:
            post_date = 'Unavailable'
        try:
            edit_date = soup.find('span', class_='DateTime lia-message-edited-on lia-component-common-widget-date')\
                .find('span', class_='message_post_text').text
        except AttributeError:
            edit_date = 'Unedited'
        start = get_thread_page_numbers(soup)
        end = 1
        now = datetime.now()
        post_total = str(10 * start)
        try:
            op = soup.find('div', class_='thread-main-header')
        except:
            op = None
        try:
            author = op.find('a', class_='lia-link-navigation lia-page-link lia-user-name-link user_name').find('span').text
        except:
            author = ''

        try:
            author_url = 'https://community.upwork.com/' 
            author_url += str(op.find('a', class_='lia-link-navigation lia-page-link lia-user-name-link user_name')).split('href="')[1].split('"')[0]
            author_url, author, author_joindate, author_rank, thread_author_id = parse_profile(author_url, author)
        except:
            author_url, author, author_joindate, author_rank, thread_author_id = None, None, None, None, None
        thread_id = str(hashlib.md5((url + post_date + author).encode('utf-8')).hexdigest())
        if op is not None:
            for msg in op:
                try:
                    post_total = msg.find('span', class_='MessagesPositionInThread').text.split('of ')[1].replace('\n', '').replace(',', '')
                    break
                except:
                    pass
        count = 0
        for pagenum in range(start, end-1, -1):
            if pagenum > 1:
                r = requests.get(generate_next(url, pagenum))
                soup = BeautifulSoup(r.content, 'html.parser')
            else:
                r = requests.get(url)
                soup = BeautifulSoup(r.content, 'html.parser')

            msgli, count = get_message_divs(soup, categ, url)
            queue = []
            for msg in msgli:
                if msg is None:
                    continue
                try:
                    post, timestamp, edit_date, url, pagenum, index, name, user_url, member_since, rank, \
                    editor_url, editor_name, editor_joindate, editor_rank, editor_id, edit_status = parse_message_div(msg, url, pagenum)
                except:
                    continue
                count += 1
                date_format = "%b %d, %Y %I:%M:%S %p"
                dt = datetime.strptime(timestamp, date_format)
                try:
                    et = datetime.strptime(edit_date, date_format)
                except:
                    et = None
                author_id = str(hashlib.md5((url + name + member_since + rank).encode('utf-8')).hexdigest())
                insert_from_user(author_id, name, user_url, member_since, rank)
                insert_from_user(editor_id, editor_name, editor_url, editor_joindate, editor_rank)
                insert_post(thread_id, post, dt, et, author_id, editor_id, edit_status, pagenum, int(index))
        try:
            assert(count >= ((start * 10)-10) and count <= start * 10)
        except:
            print(count, start, url)
        date_format = "%b %d, %Y %I:%M:%S %p"
        insert_thread(thread_id, categ, url, title, datetime.strptime(post_date, date_format), thread_author_id, start, int(count))

def parse_profile(url, name):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    data_container = soup.find('div', class_='userdata-combine-container')
    if data_container is not None:
        joininfo = data_container.find_all('span', class_='member-info')
        for entry in joininfo:
            if entry.text != 'Member since:':
                joindate = entry.text
        rank_container = data_container.find('div', class_='user-userRank')
        rank = rank_container.text.strip()

        return url, name, joindate, rank, str(hashlib.md5((url + name + joindate + rank).encode('utf-8')).hexdigest())
    else:
        return None
def get_message_divs(soup, categ, url):
    try:
        op = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-topic')
    except:
        op = None
    
    try:
        unread = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply')
    except:
        unread = None
    
    try:
        solved = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-list-row-thread-solved')
    except:
        solved = None

    try:
        resolved = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-topic lia-list-row-thread-solved')
    except:
        resolved = None
    
    try:
        solution = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-list-row-thread-solved lia-accepted-solution')
    except:
        solution = None

    try:
        no_content = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-message-with-no-content')
    except:
        no_content = None

    try:
        solved_no_content = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-list-row-thread-solved lia-message-with-no-content')
    except:
        solved_no_content = None

    try:
        readonly = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-topic lia-list-row-thread-readonly')
    except:
        readonly = None

    try:
        readonlyreply = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-list-row-thread-readonly')
    except:                                     
        readonlyreply = None

    try:
        solvedreadonly = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-list-row-thread-solved lia-list-row-thread-readonly')
    except: 
        solvedreadonly = None

    try:
        other = soup.find_all('div', class_='MessageView lia-message-view-forum-message lia-message-view-display lia-row-standard-unread lia-thread-reply lia-message-authored-by-you')
    except:
        other = None

    msgs = op + unread + solved + no_content + resolved + solution + readonly + readonlyreply + solvedreadonly + other + solved_no_content

    msgli = []
    for msg in msgs:
        msgli.append(msg)

    return reversed(msgli), len(msgli)

def parse_message_div(msg, url, pagenum):
    edit_status = 'Unedited'
    try:
        _url = 'https://community.upwork.com' + \
            msg.find('a', class_='lia-link-navigation lia-page-link lia-user-name-link user_name', href=True)['href']
    except:
        _url = '**URL Unavailable**'

    try:
        name = msg.find('a', class_='lia-link-navigation lia-page-link lia-user-name-link user_name').find('span').text
    except:
        name = '**Deleted Profile**'
    try:
        member_since = msg.find('span', class_='custom-upwork-member-since').text.split(': ')[1]
    except:
        member_since = '**Joindate Unavailable**'
    try:
        rank = msg.find('div', class_='lia-message-author-rank lia-component-author-rank lia-component-message-view-widget-author-rank')\
            .text.replace(' ', '').strip()
    except:
        rank = '**Rank Unavailable**'
        
    dateheader = msg.find('p', class_='lia-message-dates lia-message-post-date lia-component-post-date-last-edited lia-paging-page-link custom-lia-message-dates')
        
    if dateheader is not None:
        timestamp = dateheader.find('span', class_='DateTime lia-message-posted-on lia-component-common-widget-date')\
                    .find('span', class_='message_post_text').text
        try:
            e = dateheader.find('span', class_='DateTime lia-message-edited-on lia-component-common-widget-date')
            for span in e.find_all('span', class_='message_post_text'):
                if span.text != 'by':
                    editdate = span.text
        except:
            editdate = ''
        try:
            edited_by = dateheader.find('span', class_='username_details').find('span', class_='UserName lia-user-name lia-user-rank-Power-Member lia-component-common-widget-user-name')\
            .find('a').find('span').text
        except:
            edited_by = ''
        try:
            box = dateheader.find('span', class_='username_details').find('span', class_='UserName lia-user-name lia-user-rank-Power-Member lia-component-common-widget-user-name')\
            .find('a')
            edited_url = 'https://community.upwork.com/' 
            edited_url += str(box).split('href="')[1].split('"')[0]
        except Exception as e:
            edited_url = ''
        try:
            editor_url, editor_name, editor_joindate, editor_rank, editor_id = parse_profile(edited_url, edited_by)
        except:
            editor_url, editor_name, editor_joindate, editor_rank, editor_id = None, None, None, None, NOne
        index = msg.find('span', class_='MessagesPositionInThread').find('a').text.replace('\n', '')
        body = msg.find('div', class_='lia-message-body-content').find_all(['p', 'ul'])
        post = ''
        for p in body:
            if p.text == '&nbsp':
                pass
            if p.name == 'ul':
                li = p.find_all('li')
                for item in li:
                    post += item.text
            else:
                post += ('' + p.text + '').replace('\u00a0', '').replace('\n', '')
        if post.find('**Edited') != -1 or post.find('**edited') != -1:
            edit_status = '**Edited**'
        return post, timestamp, editdate, url, pagenum, index, name, _url, member_since, rank, \
            editor_url, editor_name, editor_joindate, editor_rank, editor_id, edit_status
    else:
        print('Couldnt find post on url:', url)
        return None

if __name__ == '__main__':
    for target in targets:
        parse_page(target)