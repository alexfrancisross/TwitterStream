from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from textblob import TextBlob
import json
import psycopg2

#connect to database
try:
    conn = psycopg2.connect("dbname='Tableau' user='Administrator' host='tableau.cuztkpb4hq8r.us-west-1.rds.amazonaws.com' password='password1'")
except:
    print "I am unable to connect to the database"

#consumer key, consumer secret, access token, access secret.
ckey="Xar3pvPceBcepR0sSEXwHv5Xf"
csecret="tR9SJfPQDJdgiL2llm6LJi00hDPzNoPtkDgqPfIXbx4VWJjTIP"
atoken="2893911603-K7qzcGrJOF4Yv5M24cky6eOfVP9h3dEm1feOaTy"
asecret="Toznia43yjPRaU3PLyg0lUbV4gOfUcCZ0IzjowvOklGLj"

#lookup arrays to convert user's timezone to a country
TimeZones = ['International Date Line West','Midway Island','American Samoa','Hawaii','Alaska','Pacific Time (US & Canada)','Tijuana','Mountain Time (US & Canada)','Arizona','Chihuahua','Mazatlan','Central Time (US & Canada)','Saskatchewan','Guadalajara','Mexico City','Monterrey','Central America','Eastern Time (US & Canada)','Indiana (East)','Bogota','Lima','Quito','Atlantic Time (Canada)','Caracas','La Paz','Santiago','Newfoundland','Brasilia','Buenos Aires','Montevideo','Georgetown','Greenland','Mid-Atlantic','Azores','Cape Verde Is.','Dublin','Edinburgh','Lisbon','London','Casablanca','Monrovia','UTC','Belgrade','Bratislava','Budapest','Ljubljana','Prague','Sarajevo','Skopje','Warsaw','Zagreb','Brussels','Copenhagen','Madrid','Paris','Amsterdam','Berlin','Bern','Rome','Stockholm','Vienna','West Central Africa','Bucharest','Cairo','Helsinki','Kyiv','Riga','Sofia','Tallinn','Vilnius','Athens','Istanbul','Minsk','Jerusalem','Harare','Pretoria','Kaliningrad','Moscow','St. Petersburg','Volgograd','Samara','Kuwait','Riyadh','Nairobi','Baghdad','Tehran','Abu Dhabi','Muscat','Baku','Tbilisi','Yerevan','Kabul','Ekaterinburg','Islamabad','Karachi','Tashkent','Chennai','Kolkata','Mumbai','New Delhi','Kathmandu','Astana','Dhaka','Sri Jayawardenepura','Almaty','Novosibirsk','Rangoon','Bangkok','Hanoi','Jakarta','Krasnoyarsk','Beijing','Chongqing','Hong Kong','Urumqi','Kuala Lumpur','Singapore','Taipei','Perth','Irkutsk','Ulaanbaatar','Seoul','Osaka','Sapporo','Tokyo','Yakutsk','Darwin','Adelaide','Canberra','Melbourne','Sydney','Brisbane','Hobart','Vladivostok','Guam','Port Moresby','Magadan','Srednekolymsk','Solomon Is.','New Caledonia','Fiji','Kamchatka','Marshall Is.','Auckland','Wellington',"Nuku'alofa",'Tokelau Is.','Chatham Is.','Samoa','Europe/London','America/New_York','EST']
Country = ['United States Minor Outlying Islands','United States Minor Outlying Islands','American Samoa','United States','United States','United States','Mexico','United States','United States','Mexico','Mexico','United States','Canada','Mexico','Mexico','Mexico','Guatemala','United States','United States','Colombia','Peru','Peru','Canada','Venezuela','Bolivia','Chile','Canada','Brazil','Argentina','Uruguay','Guyana','Greenland','South Georgia and the South Sandwich Islands','Portugal','Cape Verde','Ireland','United Kingdom','Portugal','United Kingdom','Morocco','Liberia','#N/A','Serbia','Slovakia','Hungary','Slovenia','Czech Republic','Bosnia and Herzegovina','Macedonia','Poland','Croatia','Belgium','Denmark','Spain','France','Netherlands','Germany','Germany','Italy','Sweden','Austria','Algeria','Romania','Egypt','Finland','Ukraine','Latvia','Bulgaria','Estonia','Lithuania','Greece','Turkey','Belarus','Israel','Zimbabwe','South Africa','Russia','Russia','Russia','Russia','Russia','Kuwait','Saudi Arabia','Kenya','Iraq','Iran','Oman','Oman','Azerbaijan','Georgia','Armenia','Afghanistan','Russia','Pakistan','Pakistan','Uzbekistan','India','India','India','India','Nepal','Bangladesh','Bangladesh','Sri Lanka','Kazakhstan','Russia','Myanmar','Thailand','Thailand','Indonesia','Russia','China','#N/A','Hong Kong','China','Malaysia','Singapore','Taiwan','Australia','Russia','Mongolia','South Korea','Japan','Japan','Japan','Russia','Australia','Australia','Australia','Australia','Australia','Australia','Australia','Russia','Guam','Papua New Guinea','Russia','Russia','Solomon Islands','New Caledonia','Fiji','Russia','Marshall Islands','New Zealand','New Zealand','Tonga','Tokelau','New Zealand','Samoa','United Kingdom','United States','United States']

#listner for twitter stream
class listener(StreamListener):
        def on_data(self, data):
           # print(data)
            all_data = json.loads(data)

            try:
                tweet = TextBlob(all_data["text"])
                print(tweet.raw)

                # determine if sentiment is positive, negative, or neutral
                if tweet.sentiment.polarity < 0:
                    sentiment = "negative"
                elif tweet.sentiment.polarity == 0:
                    sentiment = "neutral"
                else:
                    sentiment = "positive"

                created_at = all_data["created_at"]
                favorite_count = all_data["favorite_count"]
                favorited = all_data["favorited"]
                filter_level = all_data["filter_level"]
                id_str = all_data["id_str"]
                lang = all_data["lang"]
                retweet_count = all_data["retweet_count"]
                retweeted = all_data["retweeted"]
                source = all_data["source"]
                timestamp_ms = all_data["timestamp_ms"]
                truncated = all_data["truncated"]
                user_description = all_data["user"]["description"]
                user_favourites_count = all_data["user"]["favourites_count"]
                user_followers_count = all_data["user"]["followers_count"]
                user_friends_count = all_data["user"]["friends_count"]
                user_id_str = all_data["user"]["id_str"]
                user_location = all_data["user"]["location"]
                user_name = all_data["user"]["name"]
                user_profile_image_url = all_data["user"]["profile_image_url"]
                user_screen_name = all_data["user"]["screen_name"]
                user_statuses_count = all_data["user"]["statuses_count"]
                user_time_zone = all_data["user"]["time_zone"]

                #convert hastags array into comma separated string
                hashtags = all_data["entities"]["hashtags"]
                hashtags_str = ""
                for i in range(0,len(hashtags),1):
                    hashtags_str = unicode(hashtags_str) + hashtags[i]["text"]
                    if i + 1 < len(hashtags):
                        hashtags_str = hashtags_str + ","

                #convert urls into comma separated strinf
                urls = all_data["entities"]["urls"]
                urls_str = ""
                for i in range(0,len(urls),1):
                    urls_str = unicode(urls_str) + urls[i]["url"]
                    if i + 1 < len(urls):
                        urls_str = urls_str + ","

                #convert user timezone into country
                user_timezone_country = ''
                if user_time_zone in TimeZones:
                    index = TimeZones.index(user_time_zone)
                    user_timezone_country= Country[index]
                c = conn.cursor()

                #insert data into database
                c.execute("INSERT INTO Twitter (tweet,created_at,timestamp_ms,favorite_count,favorited,filter_level,id_str,lang,retweet_count,retweeted,source,truncated,"
                          "user_description,user_favourites_count,user_followers_count,user_friends_count,user_id_str,user_location,user_name,user_profile_image_url,"
                          "user_screen_name,user_statuses_count,user_time_zone,hashtags,urls,user_timezone_country,polarity,sentiment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (tweet.raw,created_at,timestamp_ms,favorite_count,favorited,filter_level,id_str,lang,retweet_count,retweeted,source,truncated,user_description,user_favourites_count,
                     user_followers_count,user_friends_count,user_id_str,user_location,user_name,user_profile_image_url,user_screen_name,user_statuses_count,user_time_zone,hashtags_str,urls_str,user_timezone_country,tweet.sentiment.polarity,sentiment))

                conn.commit()

                return(True)

            except Exception as e:
                print(e)
                print (all_data)

            def on_error(self, status):
                print status


auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)


while True:
    try:
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=['@tableau','#tableau','#data15']) #insert keywords for Twitter search
        #twitterStream.sample() #random sample from twitter

    except:
        # Oh well, reconnect and keep going
        continue

