import tweepy
import twitter
# These keys are generated by twitter
# Documentation is available on wiki

consumer_key="hI0cucQe847sNyLeeAP8dQ"
consumer_secret="2OHFiDXlw1ZRuEQJhT2o44R7cf6Wt6tFsufqC6kq8qE"
access_token_key="1872819308-FpjKJJQw39MeFAL4MsENWirmVPcp98ijOwV8e28"
access_token_secret="aN6ZodKeOoMq3dn7XLbEqVf32ayoaTWQ4LhxPRYERukLv"


def main():
	#print "Hello"
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token_key, access_token_secret)
	#api = tweepy.API(auth)
	api = twitter.Api(
				consumer_key="hI0cucQe847sNyLeeAP8dQ",
				consumer_secret="2OHFiDXlw1ZRuEQJhT2o44R7cf6Wt6tFsufqC6kq8qE",
				access_token_key="1872819308-FpjKJJQw39MeFAL4MsENWirmVPcp98ijOwV8e28",
				access_token_secret="aN6ZodKeOoMq3dn7XLbEqVf32ayoaTWQ4LhxPRYERukLv"
				)
	#print "User name : ",api.me().name
	users = api.GetFriends()
	print [u.name for u in users]

if __name__ == '__main__':
	main()
