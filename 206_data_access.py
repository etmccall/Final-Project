
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

try:
	cache_file = open(CACHE_FNAME,'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {} 


def request_movie(x):
	if x in CACHE_DICTION: 
		movie_results = CACHE_DICTION[x]
	else: 
		base_url = "http://www.omdbapi.com/?" # fill in
		params_diction = {} # you'll need to decide what key-val pairs go in here
		params_diction["t"] = x
		movie_results=requests.get(base_url,params=params_diction).text 
		CACHE_DICTION[x]=movie_results
		cache_file = open(CACHE_FNAME,'w') 
		cache_file.write(json.dumps(CACHE_DICTION))
		cache_file.close()
	return json.loads(movie_results)



zoo=request_movie("Zootopia")
dead=request_movie("Deadpool")
dark_knight=request_movie("The Dark Knight") 
movie_list=[zoo,dead,dark_knight]



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
		return "{} is a movie featuring {} and it imdb rating is {}.".format(self.title,self.actors, self.rating)
 
three_movie_class=[Movie(i) for i in movie_list]


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

def get_users(l): 
	y=[]
	y.append(l['user']['screen_name'])
	z=l['entities']['user_mentions'] 
	if z !=[]: 
		for x in z: 
			y.append(x['screen_name'])
	return y

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
quer="Select text FROM Tweets WHERE retweets > 20"
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

cur.execute('SELECT movie_title, rating FROM Movies');
ratings_dic={}
res = cur.fetchall()
for x in res: 
	ratings_dic[x[0]]=x[1]
items = ratings_dic.items();
sorted_items = sorted(items, key = lambda x: x[1], reverse=True) 

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
 
f = open("206_Final_Project.txt",'w')
f.close()
if os.stat("206_Final_Project.txt").st_size == 0: 
	m = open("206_Final_Project.txt", 'w') 
	m.write("This file contains information on the Deadpool, Zootopia, and The Dark Knight. It lists their quality, their imdb ratings, popular tweets about them, and active users involved in a tweet about one of the movies.")
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
	x=0
	for wmm in movie_diction.keys():
		m.write(wmm) 
		m.write(": ") 
		for wrt in movie_diction[wmm]:
			if x != 0:
				m.write(", ")
			m.write(wrt)
			x = x +1
		m.write("\n")
	m.write("Below is a list of active users who tweeted about one of the movies. An active user favorited over 10,000 tweets. Each item is organized by movie, screen name, number of user's favorites, and number of retweets the tweet received.")
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


class Test1(unittest.TestCase):
	def test_request_movie(self): 
		d={}
		m=request_movie("Zootopia")
		self.assertEqual(type(m),type(d), "Testing that request_movie fucnction returns a dictionary")
	def test_request_movie2(self):
		d={}
		m=request_movie("Zootopia")
		self.assertEqual(type(list(m.keys())[0]),type(""), "Testing that the keys in the dictionary are strings")
class Test2(unittest.TestCase):
	def test_characterize_rating(self):
		self.assertEqual(type(three_movie_class[0].characterize_rating()),type("Poor"), "Testing that characterize rating method in movie class returns a string")
	def test_characterize_rating2(self): 
		three_movie_class[0].rating=3.0
		self.assertEqual(three_movie_class[0].characterize_rating(),"Poor", "Testing that characterize rating method returns correct word based of value")
class Test3(unittest.TestCase):
	def test_omdb_rating(self): 
		self.assertEqual(type(three_movie_class[0].rating),type(8.1),"Testing that rating is an integer")
class Test6(unittest.TestCase):
	def test_user_info2(self):
		r = get_twitter_movie_infor("Zootopia")[0]
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(d),type(r), "Testing that test_movie_infor returns a list whose elements are dictionaries")
	def test_user_info1(self):
		r = get_twitter_movie_infor("Zootopia")
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(m),type(r),"Testing that test_movie_infor returns a list")
class Test7(unittest.TestCase):
	def test_is_comedy(self):
		l = request_movie("Zootopia")
		self.assertEqual(type(Movie(l).is_comedy()),type("what"), "Testing is_comedy() method of movie class returns a string")
	def test_is_comedy2(self):
		l = request_movie("Zootopia")
		rry = Movie(l)
		rry.genre = ["Animation", "Comedy", "Action"]
		self.assertEqual(Movie(l).is_comedy(),"Comedy", "Testing that is_comedy() returns a comedy appropiately")
class Test8(unittest.TestCase):
	def test_three_movie_class1(self): 
		d={}
		f={}
		m=[d,f]
		self.assertEqual(type(m),type(movie_list), "Testing that movie list is a list")
	def test_three_movie_class2(self): 
		d={}
		f={}
		l = request_movie("Zootopia")
		self.assertEqual(type(Movie(l)),type(three_movie_class[0]), "Testing that first element of three_movie_class is a Movie class type")
class Test9(unittest.TestCase):
	def test_caching(self):
		fstr = open("SI206_final_project_cache.json","r").read()
		self.assertTrue("Zootopia" in fstr, "Testing that the caching works properly")
class Test10(unittest.TestCase):
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
		cur.execute('SELECT user_posted FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(len(result[1][0])>=2,"Testing that a tweet user_id is a valid Twitter user_id")
		conn.close()
	def test_tweets_4(self):
		conn = sqlite3.connect('final_project.db')
		cur = conn.cursor()
		cur.execute('SELECT tweet_id FROM Tweets');
		result = cur.fetchall()
		self.assertTrue(result[0][0] != result[15][0], "Testing that the table consists of unique tweets")
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
class Test10(unittest.TestCase):
	def test_movie_diction(self): 
		self.assertEqual(type(movie_diction), type({}), "Testing that movie_diction is a dictionary")
	def test_movie_diction1(self):
		x = 0 
		for rwy in movie_diction.values(): 
			x = x + 1
		self.assertEqual(type(rwy), type([]), "Testing that movie_diction values are lists")
	def test_res(self): 
		self.assertEqual(type(res[0]), type(()), "Testing cur fetchall properly returned a tuple") 
	def test_sorted(self): 
		self.assertGreater(sorted_items[0][1], sorted_items[1][1], "Testing movies are listed in increasing order")
	def test_join_query(self): 
	 	self.assertEqual(len(ress[0]),4, "Testing that the length of each tuple in fetch all is 4")
	def test_join_query2(self): 
	 	self.assertGreater(ress[0][2],10000, "Testing that user's number of favorites in joined query is greater than 10000") 
	def test_join_query3(self):
	 	self.assertEqual(type(tweets_movie_dictionn[0]), type(()), "Testing that tweet_diction is a list of tuples")
	def test_join_query4(self):
	 	self.assertEqual(len(tweets_movie_dictionn[0]), 4, "Testing that length of each tuple in tweet_diction is 4")

class Test_Movie_qual(unittest.TestCase):
	def test_quality_movies(self):
		self.assertEqual(type(quality_movies),type(["hi","Bye"]),"Testing that quality_movies is a list")

class Test_Txt_file(unittest.TestCase):
	def test_txt(self): 
		fstr = open("206_Final_Project.txt","r").read()
		self.assertTrue("Zootopia" in fstr, "Testing that Zootopia was written to file ")
	def test_txt1(self): 
		fstr = open("206_Final_Project.txt","r").read()
		self.assertTrue("," in fstr, "Testing that the file contains commas")
	def test_txt2(self): 
		fstr = open("206_Final_Project.txt","r").read()
		self.assertTrue(":" in fstr, "Testing that the file contains : ")
	def test_txt3(self): 
		fstr = open("206_Final_Project.txt","r").read()
		self.assertTrue("Below" in fstr, "Testing that the file contains Below ")

if __name__ == "__main__":
	unittest.main(verbosity=2)

# Remember to invoke your tests so they will run! (Recommend using the verbosity=2 argument.)