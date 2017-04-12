from django.shortcuts import render
from django.http import HttpResponse
from cassandra.cluster import Cluster


def index(request):
	cluster = Cluster(port=9043)
	session = cluster.connect()
	
	rows = session.execute('SELECT * FROM musicnet_metadata.chord;')
	
	if request.method == 'GET':
		text = request.GET.get("Key","")
		
	s = ""
		
	for i in rows:
		if text == "*":
			s +=  str(i.id) + " " + i.song_key +  "<br>"
		elif i.chord_value == text:
		#	meta = session.execute('SELECT * FROM musicnet_metadata.metadata WHERE id=' + str(i.id))
			s +=  str(i.id) + " " + i.chord_value +  "<br>"
			
	return HttpResponse(s)
# Create your views here.

#~ Search song by key
#~ Search song by chord progression
#~ Search by song name and pull up other metadata info + key + chords
#~ Search by composer and pull up names of all their songs
