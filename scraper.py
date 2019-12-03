import requests_oauthlib
import requests
import sys
from datetime import datetime
import socket
import time
import json
import oauth2 as oauth
import threading

consumer_key = ""
consumer_secret = ""

access_key = ""
access_secret = ""

HOST = ''
PORT = 9994

auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_key, access_secret)


def read_data(conn):
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	# specify location in box, south west comes first, longitude, latitude
	data = [('language', 'en'), ('locations', '-141,45,-19,90')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
	response = requests.get(query_url, auth=auth, stream=True)
	count = 0
	for line in response.iter_lines():
		try:
			# basic filter
			if not line.strip():
				continue
			if count > 1000000:
				break
			# post = json.loads(line.decode('utf-8'))
			# print(post)
			count += 1
			conn.send(line+b'\n')
			# print(str(datetime.now())+' '+'count:'+str(count))
		except json.decoder.JSONDecodeError as e:
			print(e)
			e = sys.exc_info()[0]
			print("Error: %s" % e)
	conn.close()


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Socket created')

# Bind socket to local host and port
try:
	s.bind((HOST, PORT))
except socket.error as msg:
	# print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
	print(msg)
	sys.exit()

print('Socket bind complete')

# Start listening on socket
s.listen(10)
print('Socket now listening')


# now keep talking with the client
while 1:
	# wait to accept a connection - blocking call
	conn, addr = s.accept()
	print('Connected with ' + addr[0] + ':' + str(addr[1]))

	# start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
	t = threading.Thread(target=read_data, args=(conn,))
	t.start()

s.close()