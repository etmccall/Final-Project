###### INSTRUCTIONS ###### 

# An outline for preparing your final project assignment is in this file.

# Below, throughout this file, you should put comments that explain exactly what you should do for each step of your project. You should specify variable names and processes to use. For example, "Use dictionary accumulation with the list you just created to create a dictionary called tag_counts, where the keys represent tags on flickr photos and the values represent frequency of times those tags occur in the list."

# You can use second person ("You should...") or first person ("I will...") or whatever is comfortable for you, as long as you are clear about what should be done.

# Some parts of the code should already be filled in when you turn this in:
# - At least 1 function which gets and caches data from 1 of your data sources, and an invocation of each of those functions to show that they work 
# - Tests at the end of your file that accord with those instructions (will test that you completed those instructions correctly!)
# - Code that creates a database file and tables as your project plan explains, such that your program can be run over and over again without error and without duplicate rows in your tables.
# - At least enough code to load data into 1 of your dtabase tables (this should accord with your instructions/tests)

######### END INSTRUCTIONS #########

# Put all import statements you need here.

# Begin filling in instructions....
import unittest
import itertools
import collections
import twitter_info
import tweepy
import json
import sqlite3
import omdb
import requests
import omdb
import re
import os


##The first code in the data file is code to access the twitter api. 


consumer_key = "mjRy0eJMqyuTAHcHgMSf6nBnw" 
consumer_secret = "VYEMPBqHjHI3fZbXpL6NliKd02JyxMkeONGBAElgaOlhX5RiJK"
access_token = "1403022116-6TNaHF2n9GxD1Ga35XMq9HYbAR4BWWPJP8bUMnE"
access_token_secret = "YhcITP1nGO7deWexc7HkrCZhmiJ2t5Laaq8wE6oEgMx2H"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser()) 
public_tweets = api.home_timeline()

 
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

CACHE_FNAME = "SI206_final_project_cache.json"
##Below sets up the caching pattern 
try:
	cache_file = open(CACHE_FNAME,'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {} 

##The function get request_movie takes an argument of a movie. It checks to see if the movie is a key in the dictionary. If it isn't, it searches the 
##omdb api for it and stores the resulting dictonary as a value paired with the movie title as a key. 

def request_movie(x):
	if x in CACHE_DICTION: 
		movie_results = CACHE_DICTION[x]
	else: 
		base_url = "http://www.omdbapi.com/?" # fill in
		params_diction = {} # you'll need to decide what key-val pairs go in here
		params_diction["t"] = x
		movie_results=requests.get(base_url,params=params_diction).text 
		CACHE_DICTION[x]=movie_results
		cache_file = open(CACHE_FNAME, 'w') 
		cache_file.write(json.dumps(CACHE_DICTION))
		cache_file.close()
	return json.loads(movie_results)

##Movie_list is a list of the information from the omdb api of three movies. 

zoo=request_movie("Zootopia")
dead=request_movie("Deadpool")
dark_knight=request_movie("The Dark Knight") 
movie_list=[zoo,dead,dark_knight]

##The movie class creates instance variables and methods for each movie based off the information in the omdb api. 

class Movie(object):
	def __init__(self, d):
		self.actors = d['Actors']
		self.rating = float(d['imdbRating'])
		self.title = d['Title']
		self.director=d['Director']
		self.genre = d['Genre']
	def characterize_rating(self): 
		if self.rating > 8.5: 
			return "Excellent"
		elif self.rating > 7:
			return "Good"
		elif self.rating > 5:
			return "Average"
		else:
			return "Poor"
	def is_comedy(self): 
		mq= [s.strip() for s in self.genre.split(",")] 
		if "Comedy" in mq: 
			return "Comedy"
		else: 
			return "Not Comedy"
	def __str__(self):
		return "{} is a movie featuring {} and it omdb rating is {}.".format(self.title,self.actors, self.rating)

##The list three_movie_class creates a list of three movie objects. 
three_movie_class=[Movie(i) for i in movie_list]

## The function get_twitter_movie_infor takes a movie as an argument, gives it a special format that it checks to see if it is in the CACHE_Diction. If
## it isn't, it searches the twitter api for 5 tweets using the movie as search term. It stores the resulting list as a value paired with the movie title 
##in a special format as a key.
def get_twitter_movie_infor(z): 
	unique_identifier = "twitter_{}".format(z)
	if unique_identifier in CACHE_DICTION:
		python_obj_data = CACHE_DICTION[unique_identifier]
	else: 
		results = api.search(q=z)
		list_of_arnold = results["statuses"]
		python_obj_data = list_of_arnold[:5]  
		CACHE_DICTION[unique_identifier] = python_obj_data
		cache_file = open(CACHE_FNAME, 'w') 
		cache_file.write(json.dumps(CACHE_DICTION))
		cache_file.close()
	return python_obj_data

##The function get_users takes a tweet as an argument and returns a list of all users involved in the tweet. 
def get_users(l): 
	y=[]
	y.append(l['user']['screen_name'])
	z=l['entities']['user_mentions'] 
	if z !=[]: 
		for x in z: 
			y.append(x['screen_name'])
	return y

## The following code creates the Movies table, using the three_movie_class list. Its columns are the title, the actors, the director, the rating, 
## and the quality for each movie through using the instance variables and methods from the class movie. 
conn = sqlite3.connect('final_project.db')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS Movies')



table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Movies (movie_title TEXT PRIMARY KEY, '
table_spec += 'actors TEXT, director TEXT, rating FLOAT, quality TEXT, comedy TEXT)'
cur.execute(table_spec)
statement = 'DELETE FROM Movies'
cur.execute(statement)
conn.commit()
statement = 'INSERT INTO Movies VALUES (?, ?, ?, ?,?, ?)'
for mm in three_movie_class: 
	tupz = (mm.title, mm.actors,mm.director, mm.rating, mm.characterize_rating(), mm.is_comedy())
	cur.execute(statement, tupz)
conn.commit()

## The following code creates the Users table. It searches for tweets for movies in the three_movie_class list through accessing  the title and using
## the get_movie_infor function. It then uses get_users function to get each user. It appends the first user to a list to access in the tweets table
## because this is the author of the tweet. It the uses the twitter api to get information on the twitter user that it stores in columns. The columns of 
## this table are the user's id, screen name, number of times they favorites a tweet, and their twitter description. 

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Users (user_id TEXT, '
table_spec += 'screen_name TEXT, num_favs INTEGER, description TEXT)'
cur.execute(table_spec)
statement = 'DELETE FROM Users'
cur.execute(statement)
conn.commit()
statement = 'INSERT INTO Users VALUES (?, ?, ?, ?)'
l=[]
for i in three_movie_class: 
	mz = get_twitter_movie_infor(i.title)
	for xx in mz:
		rr = get_users(xx)
		l.append(rr[0])
		for tq in rr: 
			ll=api.get_user(tq)
			tupz=(ll['id_str'],tq,ll['favourites_count'],ll["description"])
			cur.execute(statement, tupz)
conn.commit()

## The following code creates the Tweets table. It searches for tweets for movies in the three_movie_class list through accessing an the title and using
## the get_movie_infor function and stores all the tweets in a list. It then creates a table that contain information on the tweet such as the tweet id
## the user who posted it, the tweet text, the number of times the tweet has been retweeed, the number of times the tweet has been favorited by using
## the keys in each tweet's dictory and using the list of users who posted the tweet. 
table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Tweets (tweet_id INTEGER PRIMARY KEY, '
table_spec += 'user_posted TEXT, text TEXT, retweets INTEGER, favorites INTEGER)'
cur.execute(table_spec)
statement = 'DELETE FROM Tweets'
cur.execute(statement)
conn.commit()
statement = 'INSERT INTO Tweets VALUES (?, ?, ?, ?,?)'
y=[m for i in three_movie_class for m in get_twitter_movie_infor(i.title)]
for qq in range(0,15):  
	tupz=(y[qq]["id"], l[qq], y[qq]["text"], y[qq]['retweet_count'], y[qq]['favorite_count'])
	cur.execute(statement, tupz)
conn.commit()

##The following code selects movie titles from the list of movies who received an excellent rating. It sorts this list alpahetbically. 
query='Select movie_title FROM Movies WHERE instr(quality,"Ex")'
cur.execute(query)
quality_movie= cur.fetchall()
quality_movies=[]

for x in quality_movie:
	quality_movies.append(x[0])

##The following code selects texts from popular tweets. It then uses regular expressions to find the movies mentioned in the tweet and creates a 
##dictionary with the movie as a key, and the values as list of tweets about them. 
quer="Select text FROM Tweets WHERE retweets + favorites >= 20"
cur.execute(quer)
popular_movie= cur.fetchall()
popular_movies=[x[0] for x in popular_movie]

cur.execute('SELECT movie_title FROM Movies');
resl = cur.fetchall()
title_list=[x[0] for x in resl]

strtitle_list=""
for x in popular_movies: 
	strtitle_list= strtitle_list + "\n"+ x 


movie_diction={}
for moviee in title_list: 
	xxx=""	
	xxx= ".*"+moviee+".*"
	m=re.findall(xxx,strtitle_list)
	xxxx= ".*"+moviee.upper()+".*"
	my =re.findall(xxxx,strtitle_list)
	c = m + my
	if c != []:
		movie_diction[moviee] = c

##The following code selects movie titles and ratings from the list of movies, and organzies these tuples in a list sorted by the highest rating. 
cur.execute('SELECT movie_title, rating FROM Movies');
ratings_dic={}
res = cur.fetchall()
for x in res: 
	ratings_dic[x[0]]=x[1]
items = ratings_dic.items();
sorted_items = sorted(items, key = lambda x: x[1], reverse=True) 

## The following inner join gets information from the users and tweets table, joined by user name in the cases where the user favorited over 10,000 
## tweets. It then uses regular expressions to find the movie mentioned in the text. It then creates a list of tuples that contain the movie, the user
## the times the user has favorited a tweet, and the number of tweets the user's retweet received. 
m2q="SELECT Tweets.text, Users.screen_name, Users.num_favs, Tweets.retweets FROM Users INNER JOIN Tweets on Tweets.user_posted=Users.screen_name WHERE Users.num_favs > 10000";
cur.execute(m2q)
ress= cur.fetchall()
tweets_movie_dictionn=[]
for movs in title_list: 
	y = len(movs) 
	if re.match("The",movs): 
		mm =movs[4:y]
		title_list.append(mm)
for xw in ress: 
	z = xw[0]
	for mov in title_list: 
		list_of_inf=(mov,xw[1],xw[2], xw[3])
		xxx= ".*"+mov+".*"
		xxxx= ".*"+mov.upper()+".*" 
		xmy = ".*"+mov.lower()+".*"
		if re.search(xxx, z): 
			tweets_movie_dictionn.append(list_of_inf) 
		if re.search(xxxx, z): 
			tweets_movie_dictionn.append(list_of_inf)  
		if re.search(xmy, z): 
			tweets_movie_dictionn.append(list_of_inf)

## The following writes the information gathered from the queries to a txt file. 
f = open("206_Final_Project.txt",'w')
f.close()
if os.stat("206_Final_Project.txt").st_size == 0: 
	m = open("206_Final_Project.txt", 'w') 
	m.write("This file contains information on the Deadpool, Zootopia, and The Dark Knight that lists their quality, their imdb ratings, popular tweets about them, and lists of users who have favorites a lot of tweets and tweeted about them")
	m.write("\n")
	m.write("These movies received an excellent rating from Imdb")
	m.write("\n")
	for x in quality_movies: 
		m.write(x)
		m.write("\n")
	m.write("Below is a list of the movies ranked by quality")
	m.write("\n")	
	for z in sorted_items: 
		m.write(z[0]) 
		m.write(", ") 
		m.write(str(z[1]))
		m.write("\n")
	m.write("Below is a list of popular tweets about one of the movies")
	m.write("\n")
	for wmm in movie_diction.keys():
		m.write(wmm) 
		m.write(": ") 
		for wrt in movie_diction[wmm]:
			m.write(wrt)
			m.write(", ")
		m.write("\n")
	m.write("Below are lists about users who tweeted about a movie and have favorited a lot of tweets previously")
	m.write("\n")
	for zz in tweets_movie_dictionn: 
		m.write(zz[0]) 
		m.write(", ") 
		m.write(str(zz[1]))
		m.write(", ")
		m.write(str(zz[2]))
		m.write(", ")
		m.write(str(zz[3]))
		m.write("\n")
	m.close()	

# Put your tests here, with any edits you now need from when you turned them in with your project plan.
class Test1(unittest.TestCase):
	def test_request_movie(self): 
		d={}
		m=request_movie("Zootopia")
		self.assertEqual(type(m),type(d))
	def test_request_movie2(self):
		d={}
		m=request_movie("Zootopia")
		self.assertEqual(type(list(m.keys())[0]),type(""))
class Test2(unittest.TestCase):
	def test_characterize_rating(self):
		self.assertEqual(type(three_movie_class[0].characterize_rating()),type("Poor"))
	def test_characterize_rating(self): 
		three_movie_class[0].rating=3.0
		self.assertEqual(three_movie_class[0].characterize_rating(),"Poor")
class Test3(unittest.TestCase):
	def test_omdb_rating(self): 
		self.assertEqual(type(three_movie_class[0].rating),type(8.1),"Testing that rating is an integer")
class Test6(unittest.TestCase):
	def test_user_info2(self):
		r = get_twitter_movie_infor("Zootopia")[0]
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(d),type(r))
	def test_user_info1(self):
		r = get_twitter_movie_infor("Zootopia")
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(m),type(r))
class Test7(unittest.TestCase):
	def test_is_comedy(self):
		l = request_movie("Zootopia")
		self.assertEqual(type(Movie(l).is_comedy()),type("what"))
	def test_is_comedy2(self):
		l = request_movie("Zootopia")
		rry = Movie(l)
		rry.genre = ["Animation", "Comedy", "Action"]
		self.assertEqual(Movie(l).is_comedy(),"Comedy")
class Test8(unittest.TestCase):
	def test_three_movie_class1(self): 
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(m),type(movie_list))
	def test_three_movie_class2(self): 
		d={}
		f={}
		l = request_movie("Zootopia")
		self.assertEqual(type(Movie(l)),type(three_movie_class[0]))
class Test9(unittest.TestCase):
	def test_caching(self):
		fstr = open("SI206_final_project_cache.json","r").read()
		self.assertTrue("Zootopia" in fstr)
class Task2(unittest.TestCase):
	def test_tweets_1(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result)>=15, "Testing there are at least 15 records in the Tweets database")
		conn.close()
	def test_tweets_2(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result[1])==5,"Testing that there are 5 columns in the Tweets table")
		conn.close()
	def test_tweets_3(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT user_id FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result[1][0])>=2,"Testing that a tweet user_id value fulfills a requirement of being a Twitter user id rather than an integer, etc")
		conn.close()
	def test_tweets_4(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT tweet_id FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(result[0][0] != result[15][0], "Testing part of what's expected such that tweets are not being added over and over (tweet id is a primary key properly)...")
		if len(result) > 20:
			self.assertTrue(result[0][0] != result[20][0])
		conn.close()
	def test_users_4(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Users');
		result = cur.fetchall()
		self.assertTrue(len(result)>=2,"Testing that there are at least 2 distinct users in the Users table")
		conn.close()
	def test_users_6(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Users');
		result = cur.fetchall()
		self.assertTrue(len(result[0])==4,"Testing that there are 4 columns in the Users database")
		conn.close()
	def test_movies1(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Movies');
		result = cur.fetchall()
		self.assertTrue(len(result[0])==6,"Testing that there are 6 columns in the Movies database")
	def test_movies2(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT * FROM Movies');
		result = cur.fetchall()
		self.assertTrue(len(result)==3,"Testing that there are 3 Movies in the Movie table")
		conn.close()

class Test_Movie_qual(unittest.TestCase):
	def test_quality_movies(self):
		self.assertEqual(type(quality_movies),type(["hi","Bye"]),"Testing that quality_movies is a list")


if __name__ == "__main__":
	unittest.main(verbosity=2)

# Remember to invoke your tests so they will run! (Recommend using the verbosity=2 argument.)