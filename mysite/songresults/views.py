from django.shortcuts import render
from django.http import HttpResponse
from cassandra.cluster import Cluster


def index(request):
	cluster = Cluster(port=9043)
	session = cluster.connect()
	
	rows = session.execute('SELECT * FROM musicnet_metadata.metadata;')
	
	if request.method == 'GET':
		text = request.GET.get("Key","")
		
	s = ""
		
	for i in rows:
		if text == "*":
			s +=  str(i.id) + " " + i.song_key +  "<br>"
		elif i.composition + " " + i.movement == text:
			key = session.execute('SELECT * FROM musicnet_metadata.songkey WHERE id=' + str(i.id))
	
				
			
			for j in key:
				s += "Composition: " + i.composition + "<br>Movement: " + i.movement + \
				" <br>Composer: " + i.composer + "<br>Ensemble: " + i.ensemble + "<br>Length: " \
				+ str(i.seconds) + " seconds<br>Key: " + j.song_key + "<br>"
				
			
	return HttpResponse(s)
# Create your views here.

#~ Search song by key
#~ Search song by chord progression
#~ Search by song name and pull up other metadata info + key + chords
#~ Search by composer and pull up names of all their songs
