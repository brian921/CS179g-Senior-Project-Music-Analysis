# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark_cassandra import CassandraSparkContext
#from analyze import *
import time
import operator

class Keys:
	CMajor = [ "C", "D", "E", "F", "G", "A", "B" ]
	CshMajor = [ "C#", "D#", "F", "F#", "G#", "A#", "C" ]
	DMajor = [ "D", "E", "F#", "G", "A", "B", "C#" ]
	DshMajor = [ "D#", "F", "G", "G#", "A#", "C", "D" ]
	EMajor = [ "E", "F#", "G#", "A", "B", "C#", "D#" ]
	FMajor = [ "F", "G", "A", "A#", "C", "D", "E" ]
	FshMajor = [ "F#", "G#", "A#", "B", "C#", "D#", "F" ]
	GMajor = [ "G", "A", "B", "C", "D", "E", "F#" ]
	GshMajor = [ "G#", "A#", "C", "C#", "D#", "F", "G" ]
	AMajor = [ "A", "B", "C#", "D", "E", "F#", "G#" ]
	AshMajor = [ "A#", "C", "D", "D#", "F", "G", "A" ]
	BMajor = [ "B", "C#", "D#", "E", "F#", "G#", "A#" ]
	
	keyMap = { "C":0, "C#":1, "D":2, "D#":3, "E":4, "F":5, "F#":6, \
		"G":7, "G#":8, "A":9, "A#":10, "B":11 }
	
	@staticmethod
	def get_keys():
		keys = [ Keys.CMajor, Keys.CshMajor, Keys.DMajor, Keys.DshMajor, Keys.EMajor, Keys.FMajor, \
			Keys.FshMajor, Keys.GMajor, Keys.GshMajor, Keys.AMajor, Keys.AshMajor, Keys.BMajor ]
		return keys
		
class Chords:
	numChordsAnalyzed = 0
	chords = [ [ "C", [ "C", "E", "G" ] ], \
		[ "C#", [ "C#", "F", "G#" ] ], \
		[ "D", [ "D", "F#", "A" ] ], \
		[ "D#", [ "D#", "G", "A#" ] ], \
		[ "E", [ "E", "G#", "B" ] ], \
		[ "F", [ "F", "A", "C" ] ], \
		[ "F#", [ "F#", "A#", "C#" ] ], \
		[ "G", [ "G", "B", "D" ] ], \
		[ "G#", [ "G#", "C", "D#" ] ], \
		[ "A", [ "A", "C#", "E" ] ], \
		[ "A#", [ "A#", "D", "F" ] ], \
		[ "B", [ "B", "D#", "F#" ] ], \
		\
		[ "Cm", [ "C", "D#", "G" ] ], \
		[ "C#m", [ "C#", "E", "G#" ] ], \
		[ "Dm", [ "D", "F", "A" ] ], \
		[ "D#m", [ "D#", "F#", "A#" ] ], \
		[ "Em", [ "E", "G", "B" ] ], \
		[ "Fm", [ "F", "G#", "C" ] ], \
		[ "F#m", [ "F#", "A", "C#" ] ], \
		[ "Gm", [ "G", "A#", "D" ] ], \
		[ "G#m", [ "G#", "B", "D#" ] ], \
		[ "Am", [ "A", "C", "E" ] ], \
		[ "A#m", [ "A#", "C#", "F" ] ], \
		[ "Bm", [ "B", "D", "F#" ] ], \
		\
		[ "C*", [ "C", "D#", "F#" ] ], \
		[ "C#*", [ "C#", "E", "G" ] ], \
		[ "D*", [ "D", "F", "G#" ] ], \
		[ "D#*", [ "D#", "F#", "A" ] ], \
		[ "E*", [ "E", "G", "A#" ] ], \
		[ "F*", [ "F", "G#", "B" ] ], \
		[ "F#*", [ "F#", "A", "C" ] ], \
		[ "G*", [ "G", "A#", "C#" ] ], \
		[ "G#*", [ "G#", "B", "D" ] ], \
		[ "A*", [ "A", "C", "D#" ] ], \
		[ "A#*", [ "A#", "C#", "E" ] ], \
		[ "B*", [ "B", "D", "F" ] ], \
		\
		[ "C+", [ "C", "E", "G#" ] ], \
		[ "C#+", [ "C#", "F", "A" ] ], \
		[ "D+", [ "D", "F#", "A#" ] ], \
		[ "D#+", [ "D#", "G", "B" ] ], \
		\
		[ "Csus4", [ "C", "F", "G" ] ], \
		[ "C#sus4", [ "C#", "F#", "G#" ] ], \
		[ "Dsus4", [ "D", "G", "A" ] ], \
		[ "D#sus4", [ "D#", "G#", "A#" ] ], \
		[ "Esus4", [ "E", "A", "B" ] ], \
		[ "Fsus4", [ "F", "A#", "C" ] ], \
		[ "F#sus4", [ "F#", "B", "C#" ] ], \
		[ "Gsus4", [ "G", "C", "D" ] ], \
		[ "G#sus4", [ "G#", "C#", "D#" ] ], \
		[ "Asus4", [ "A", "D", "E" ] ], \
		[ "A#sus4", [ "A#", "D#", "F" ] ], \
		[ "Bsus4", [ "B", "E", "F#" ] ], \
		\
		[ "CM7", [ "C", "E", "G", "B" ] ], \
		[ "C#M7", [ "C#", "F", "G#", "C" ] ], \
		[ "DM7", [ "D", "F#", "A", "C#" ] ], \
		[ "D#M7", [ "D#", "G", "A#", "D" ] ], \
		[ "EM7", [ "E", "G#", "B", "D#" ] ], \
		[ "FM7", [ "F", "A", "C", "E" ] ], \
		[ "F#M7", [ "F#", "A#", "C#", "F" ] ], \
		[ "GM7", [ "G", "B", "D", "F#" ] ], \
		[ "G#M7", [ "G#", "C", "D#", "G" ] ], \
		[ "AM7", [ "A", "C#", "E", "G#" ] ], \
		[ "A#M7", [ "A#", "D", "F", "A" ] ], \
		[ "BM7", [ "B", "D#", "F#", "A#" ] ], \
		\
		[ "C7", [ "C", "E", "G", "A#" ] ], \
		[ "C#7", [ "C#", "F", "G#", "B" ] ], \
		[ "D7", [ "D", "F#", "A", "C" ] ], \
		[ "D#7", [ "D#", "G", "A#", "C#" ] ], \
		[ "E7", [ "E", "G#", "B", "D" ] ], \
		[ "F7", [ "F", "A", "C", "D#" ] ], \
		[ "F#7", [ "F#", "A#", "C#", "E" ] ], \
		[ "G7", [ "G", "B", "D", "F" ] ], \
		[ "G#7", [ "G#", "C", "D#", "F#" ] ], \
		[ "A7", [ "A", "C#", "E", "G" ] ], \
		[ "A#7", [ "A#", "D", "F", "G#" ] ], \
		[ "B7", [ "B", "D#", "F#", "A" ] ], \
		\
		[ "Cm7", [ "C", "D#", "G", "A#" ] ], \
		[ "C#m7", [ "C#", "E", "G#", "B" ] ], \
		[ "Dm7", [ "D", "F", "A", "C" ] ], \
		[ "D#m7", [ "D#", "F#", "A#", "C#" ] ], \
		[ "Em7", [ "E", "G", "B", "D" ] ], \
		[ "Fm7", [ "F", "G#", "C", "D#" ] ], \
		[ "F#m7", [ "F#", "A", "C#", "E" ] ], \
		[ "Gm7", [ "G", "A#", "D", "F" ] ], \
		[ "G#m7", [ "G#", "B", "D#", "F#" ] ], \
		[ "Am7", [ "A", "C", "E", "G" ] ], \
		[ "A#m7", [ "A#", "C#", "F", "G#" ] ], \
		[ "Bm7", [ "B", "D", "F#", "A" ] ], \
		\
		[ "Ch*7", [ "C", "D#", "F#", "A#" ] ], \
		[ "C#h*7", [ "C#", "E", "G", "B" ] ], \
		[ "Dh*7", [ "D", "F", "G#", "C" ] ], \
		[ "D#h*7", [ "D#", "F#", "A", "C#" ] ], \
		[ "Eh*7", [ "E", "G", "A#", "D" ] ], \
		[ "Fh*7", [ "F", "G#", "B", "D#" ] ], \
		[ "F#h*7", [ "F#", "A", "C", "E" ] ], \
		[ "Gh*7", [ "G", "A#", "C#", "F" ] ], \
		[ "G#h*7", [ "G#", "B", "D", "F#" ] ], \
		[ "Ah*7", [ "A", "C", "D#", "G" ] ], \
		[ "A#h*7", [ "A#", "C#", "E", "G#" ] ], \
		[ "Bh*7", [ "B", "D", "F", "A" ] ], \
		\
		[ "C*7", [ "C", "D#", "F#", "A" ] ], \
		[ "C#*7", [ "C#", "E", "G", "A#" ] ], \
		[ "D*7", [ "D", "F", "G#", "B" ] ], \
		\
		[ "CmM7", [ "C", "D#", "G", "B" ] ], \
		[ "C#mM7", [ "C#", "E", "G#", "C" ] ], \
		[ "DmM7", [ "D", "F", "A", "C#" ] ], \
		[ "D#mM7", [ "D#", "F#", "A#", "D" ] ], \
		[ "EmM7", [ "E", "G", "B", "D#" ] ], \
		[ "FmM7", [ "F", "G#", "C", "E" ] ], \
		[ "F#mM7", [ "F#", "A", "C#", "F" ] ], \
		[ "GmM7", [ "G", "A#", "D", "F#" ] ], \
		[ "G#mM7", [ "G#", "B", "D#", "G" ] ], \
		[ "AmM7", [ "A", "C", "E", "G#" ] ], \
		[ "A#mM7", [ "A#", "C#", "F", "A" ] ], \
		[ "BmM7", [ "B", "D", "F#", "A#" ] ] ]
		
	names = { "": "Major", "m":"Minor", "*":"Diminished", \
		"+":"Augmented", "sus4":"Suspended 4", "M7":"Major 7th", \
		"m7":"Minor 7th", "7":"Dominant 7th", "h*7":"Half-Diminished 7th", \
		"*7":"Diminished 7th", "mM7":"Minor-Major 7th" }

def get_song_key(songdata):
	#musicid = songdata[0]
	#data = songdata[1]
	data = songdata[1]
	
	notes = []
	counts = []
	i = 0
	while i < 12:
		#notes[i] = measuredata.filter(int(data.note_id) % 12 == i)
		notes.append([])
		j = 0
		while j < len(data):
			if data[j]["note_id"].isdigit() and int(data[j]["note_id"]) % 12 == i:
				notes[i].append(data[j])
			j += 1
		#notes.append(filter(lambda note: int(note["note_id"]) % 12 == i, measuredata))
		counts.append(0)
		j = 0
		while j < len(notes[i]): 
			counts[i] += int(notes[i][j]["end_time"]) - int(notes[i][j]["start_time"])
			j += 1
		i += 1
	
	notesUsed = [ ("C", counts[0]), ("C#", counts[1]), \
		("D", counts[2]), ("D#", counts[3]), \
		("E", counts[4]), ("F", counts[5]), \
		("F#", counts[6]), ("G", counts[7]), \
		("G#", counts[8]), ("A", counts[9]), \
		("A#", counts[10]), ("B", counts[11])]
		
	notesUsedS = sorted(notesUsed, key=operator.itemgetter(1)) # Sort by count
	notesUsedS.reverse() # Descending order
	
	musicKeys = Keys.get_keys()
	
	i = 0
	while i < 12:
		newMusicKeys = []
		for j in musicKeys:
			if notesUsedS[i][0] in j: # Make new list to filter out any keys that don't contain this note
				newMusicKeys.append(j)
		if len(newMusicKeys) > 0: # Only filter stuff out if there's still stuff left
			musicKeys = newMusicKeys
		i += 1
			
	keyBase = musicKeys[0][0]
	songKey = ""
	if notesUsed[(Keys.keyMap[keyBase] + 9) % 12][1] > notesUsed[Keys.keyMap[keyBase]][1]:
		# Check if relative minor note is used more often
		keyBase = notesUsed[(Keys.keyMap[keyBase] + 9) % 12][0]
		songKey = keyBase + " Minor"
	else:
		songKey = keyBase + " Major"
		
	#print("Song Key: " + songKey)
	
	return songKey

def get_song_chords(songdata):
	#musicid = songdata[0]
	#data = songdata[1]
	data = songdata[1]
	
	#~ i = 0
	#~ while i < len(data):
		#~ print("MEASURE: " + data[i]["measure"])
		#~ time.sleep(1)
		#~ i += 1
	
	# First, figure out how many measures there are (maybe there's a faster way to pull out the max element but idk)
	numMeasures = 0
	i = 0
	while i < len(data):
		if data[i]["measure"].isdigit() and int(data[i]["measure"]) > numMeasures:
			numMeasures = int(data[i]["measure"])
		i += 1
	
	chords = [] # List of Tuple: (Chord name, measure #, unique ID)
	
	m = 0
	while m < numMeasures:
		#measuredata = data.filter(int(data.measure) == m)
		#measuredata = filter(lambda note: note["measure"] == str(m), data)
		measuredata = []
		i = 0
		while i < len(data):
			if data[i]["measure"] == str(m):
				measuredata.append(data[i])
			i += 1
		
		#~ print("TARGET MEASURE: " + str(m))
		#~ i = 0
		#~ while i < len(measuredata):
			#~ print("MEASURE: " + measuredata[i]["measure"])
			#~ time.sleep(1)
			#~ i += 1
		# Don't bother if there's no notes in this measure
		if len(measuredata) > 0:
			notes = []
			counts = []
			i = 0
			while i < 12:
				#notes[i] = measuredata.filter(int(data.note_id) % 12 == i)
				notes.append([])
				j = 0
				while j < len(measuredata):
					if measuredata[j]["note_id"].isdigit() and int(measuredata[j]["note_id"]) % 12 == i:
						notes[i].append(measuredata[j])
					j += 1
				#notes.append(filter(lambda note: int(note["note_id"]) % 12 == i, measuredata))
				counts.append(0)
				j = 0
				while j < len(notes[i]): 
					counts[i] += int(notes[i][j]["end_time"]) - int(notes[i][j]["start_time"])
					j += 1
				i += 1
	
			notesUsed = [ ("C", counts[0]), ("C#", counts[1]), \
				("D", counts[2]), ("D#", counts[3]), \
				("E", counts[4]), ("F", counts[5]), \
				("F#", counts[6]), ("G", counts[7]), \
				("G#", counts[8]), ("A", counts[9]), \
				("A#", counts[10]), ("B", counts[11])]
				
			notesUsedS = sorted(notesUsed, key=operator.itemgetter(1)) # Sort by count
			notesUsedS.reverse() # Descending order
			
			#~ i = 0
			#~ while i < len(notesUsedS):
				#~ print(notesUsedS[i][0] + " : " + str(notesUsedS[i][1]))
				#~ i += 1
			#~ time.sleep(2)
			
			chordDict = dict() # This will map chords to a value corresponding to how well the measure "matches" the chord
			# From there we can pick the chord with the max value and declare that as the chord for this measure
			
			i = 0
			while i < len(Chords.chords):
				chordIdentifier = Chords.chords[i][0] # Name such as "Cm7"
				noteList = Chords.chords[i][1] # List of the notes in the chord
				chordDict[chordIdentifier] = 0
				n = 0
				while n < len(noteList):
					noteVal = Keys.keyMap[noteList[n]]
					noteCount = notesUsed[noteVal][1]
					chordDict[chordIdentifier] += noteCount / len(noteList)
					n += 1
				#print("MATCH VALUE: " + chordIdentifier + " : " + str(chordDict[chordIdentifier]))
				i += 1
			#time.sleep(2)
				
			maxKey = ""
			maxVal = -1	
			for k in chordDict:
				if chordDict[k] > maxVal:
					maxKey = k
					maxVal = chordDict[k]
					
			#print("CHORD GUESS: " + maxKey)
			#time.sleep(2)
					
			chords.append((maxKey, m, songdata[4] + m))
			#Chords.numChordsAnalyzed += 1
		m += 1
		
	i = 0
	chordstr = ""
	while i < len(chords):
		chordstr = " ".join([chordstr, chords[i][0]])
		i += 1
		
	#print("CHORDS: " + chordstr)
		
	return chords

def analyze_song(songdata):
	print("Analyzing song keys...")
	songdata[2] = get_song_key(songdata)
	print("Done!")
	print("Analyzing song chords...")
	songdata[3] = get_song_chords(songdata)
	print("Done!")
	#time.sleep(1)
	
	return songdata

def my_int(string):
	try:
		return int(string)
	except:
		return 0

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Usage: 2music_meta.py <file>", file=sys.stderr)
		exit(-1)
		
	numBlocks = 4
	if len(sys.argv) > 2:
		numBlocks = int(sys.argv[2])
		
	startTime = time.time()
	
	conf = SparkConf().setAppName("Music Metadata")
	sc = CassandraSparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	
	#schemas#####################################################################
	songSchema = StructType([ \
			StructField("start_time", StringType(), True), \
			StructField("end_time", StringType(), True), \
			StructField("instrument_id", StringType(), True), \
			StructField("note_id", StringType(), True), \
			StructField("measure", StringType(), True), \
			StructField("beat", StringType(), True),\
			StructField("note_value", StringType(), True),\
			StructField("song_id", StringType(), True)])
	
	metaSchema = StructType([ \
                          StructField("ID", IntegerType(), True), \
                          StructField("composer", StringType(), True), \
        	          StructField("composition", StringType(), True),\
                          StructField("movement", StringType(), True), \
                          StructField("ensemble", StringType(), True), \
                          StructField("source", StringType(), True), \
                          StructField("transcriber", StringType(), True),\
                          StructField("catalog_name", StringType(), True),\
                          StructField("seconds", IntegerType(), True)])
                          
	keySchema = StructType([ \
			StructField("ID", IntegerType(), True), \
			StructField("song_key", StringType(), True)])
			
	chordSchema = StructType([ \
			StructField("ID", IntegerType(), True), \
			StructField("chord_value", StringType(), True), \
			StructField("measure", IntegerType(), True), \
			StructField("chord_id", StringType(), True)])
	
	#load csv###################################################################
	
	songdata = [] #array of tuple: (songnumber, song csv)

	#loads all song csv's into songdata array	
	for filename in os.listdir('./dset4'):
		if filename.endswith('.csv'):
			songid = int(filename[:filename.find(".csv")])
			if songid <= 72677:
				tempsongdata = sqlContext.read.format('com.databricks.spark.csv') \
									.options(header='true') \
									.load('./dset4/' + filename, schema=songSchema)
				tempsongdata.unpersist()		
				songdata.append((songid, tempsongdata))
			
	if numBlocks == 1:
		songdata = songdata[: len(songdata) / 4]
	elif numBlocks == 2:
		songdata = songdata[: len(songdata) / 2]
	elif numBlocks == 3:
		songdata = songdata[: 3 * len(songdata) / 4]
	
	#~ keydata = sqlContext.read.format('com.databricks.spark.csv').options(header='true') \
                                  #~ .load("dummykey.csv", schema=keySchema)
                                  
	#~ chorddata = sqlContext.read.format('com.databricks.spark.csv').options(header='true') \
                                  #~ .load("dummychord", schema=chordSchema)
	
	#~ songdata = sqlContext.read.format('com.databricks.spark.csv').options(header='true') \
                                  #~ .load(sys.argv[1], schema=songSchema)
	
	meta = sqlContext.read.format('com.databricks.spark.csv').options(header='true') \
                                  .load(sys.argv[1], schema=metaSchema)
	#meta.unpersist()
	
	############################################################################
	
	#test filter: filter out all songs except those with the note value of "Sixteenth	
	#noteval = songtest.filter(songtest.note_value == "Sixteenth")
	
	songdataMapListList = [] # list of 5 tuple (musicid, songdata[] key, chords[], chordidbase)
	songSizes = []
	songsRDD = sc.parallelize([])
	songsRDD.unpersist()
	keysRDD = sc.parallelize([])
	keysRDD.unpersist()
	chordsRDD = sc.parallelize([])
	chordsRDD.unpersist()
	tempSongsRDD = sc.parallelize([])
	tempSongsRDD.unpersist()
	
	print("Analyzing 75 songs...")
	#blockstart = 0
	
	for i in range(0, len(songdata) - 1):
		
		print(str(i))
		
		musicid = songdata[i][0] # First element of tuple: music id (the number of the csv filename)
		temp1 = songdata[i][1].map(lambda row: {'start_time': row.start_time,
						'end_time': row.end_time, 
						'instrument_id': row.instrument_id, 
						'note_id': row.note_id, 
						'measure': row.measure, 
						'beat': row.beat, 
						'note_value': row.note_value, 
						'song_id': row.song_id})
		tempSongsRDD = tempSongsRDD.union(temp1)
		#~ songRDD.union(temp1)
		#~ tempSongRDD.union(temp1)
		dummyKey = ""
		dummyChords = []
		chordIDBase = i * 1024
		songdataMapListList.append([musicid, [], dummyKey, dummyChords, chordIDBase])
		
		if i % 75 == 74: # Every 75 songs, analyze each song in parallel
			
			print("COLLECTING!")
			bigList = tempSongsRDD.collect()
			tempSongsRDD = sc.parallelize([])
			print("COLLECTED!")
			print(str(len(bigList)))
			lastMeasure = -1
			
			k = 0
			for j in range(0, 75):
				while k < len(bigList) and my_int(bigList[k]["measure"]) >= lastMeasure:
					songdataMapListList[j][1].append(bigList[k])
					k += 1
					
			for j in range(0, 75):
				songdataMapListList[j][1] = songdataMapListList[j][1][: len(songdataMapListList[j][1]) / 4]
			
			rdd = sc.parallelize(songdataMapListList)
			songdataMapListList = []
			print("OK. Do analysis")
			result = rdd.map(lambda song : analyze_song(song)).collect()
			print("OK Done.")
			#songsRDD.union(result)
			
			for j in range(0, 75):
				
				musicid = result[j][0]
				songData = sc.parallelize(result[j][1])
				#songsRDD = songsRDD.union(songData)
				temp2 = sc.parallelize([ { "id": musicid, "song_key": result[j][2] } ])
				#keysRDD = keysRDD.union(temp2)
				
				
				for k in range(0, len(result[j][3]) - 1):
					
					temp3 = sc.parallelize([ { "id": musicid, "chord_value": result[j][3][k][0], "measure": str(result[j][3][k][1]), "chord_id": str(result[j][3][k][2]) } ])
					#chordsRDD = chordsRDD.union(temp3)
			
			print("Analyzing 75 songs...")
			
			#blockstart = i + 1
			
	print("Done.")
		
	#~ # Analyze each song in parallel
	#~ rdd = sc.parallelize(songdataMapListList)
	#~ #rdd.unpersist()
	#~ #rdd.foreach(analyze_song)
	#~ result = rdd.map(lambda song : analyze_song(song)).collect()
	#~ newSongdataMapListList = result
	
	#~ songdataMapList = []
	#~ keydataRDD = sc.parallelize([])
	#~ chorddataRDD = sc.parallelize([])
	
	#~ for i in range(0, len(songdata) - 1):
		#~ musicid = newSongdataMapListList[i][0]
		#~ temp1 = newSongdataMapListList[i][1]
		#~ songdataMapList.extend(temp1)
		#~ temp2 = [ { "id": musicid, "song_key": newSongdataMapListList[i][2] } ]
		#~ keydataMapList.extend(temp2)
		
		#~ for j in range(0, len(newSongdataMapListList[i][3]) - 1):
			#~ temp3 = [ { "id": musicid, "chord_value": newSongdataMapListList[i][3][j][0], "measure": str(newSongdataMapListList[i][3][j][1]), "chord_id": str(newSongdataMapListList[i][3][j][2]) } ]
			#~ chorddataMapList.extend(temp3)
	
	#~ for i in range(0, len(songdata) - 1):
		#~ musicid = songdata[i][0] # First element of tuple: music id (the number of the csv filename)
		#~ print("ANALYZING SONG: " + str(musicid))
		#~ temp1 = songdata[i][1].map(lambda row: {'start_time': row.start_time,
						#~ 'end_time': row.end_time, 
						#~ 'instrument_id': row.instrument_id, 
						#~ 'note_id': row.note_id, 
						#~ 'measure': row.measure, 
						#~ 'beat': row.beat, 
						#~ 'note_value': row.note_value, 
						#~ 'song_id': row.song_id}).collect()
		#~ songdataMapList.extend(temp1)
		#~ (songkey, chords) = analyze_song(temp1)
		#~ temp2 = [ { "id": musicid, "song_key": songkey } ]
		#~ keydataMapList.extend(temp2)
		#~ temp3 = [ { "id": musicid, "chord_value": chords[0], "measure": str(chords[1]), "chord_id": str(chords[2]) } ]
		#~ chorddataMapList.extend(temp3)
		#~ temp2 = keydata.map(lambda row: {'ID': musicid,
						#~ 'song_key': songkey}).collect()
		#~ temp3 = chorddata.map(lambda row: {'ID': musicid,
						#~ 'chord_value': chords[0],
						#~ 'measure': str(chords[1]),
						#~ 'chord_id': str(chords[2])}).collect()
		#~ sc.parallelize(temp1).saveToCassandra(keyspace='musicnet_metadata', table='song')
		#~ sc.parallelize(temp2).saveToCassandra(keyspace='musicnet_metadata', table='songkey')
		#~ sc.parallelize(temp3).saveToCassandra(keyspace='musicnet_metadata', table='chord')
		
	print("Saving stuff to cassandra...")
	time.sleep(1)
		
	#songsRDD.saveToCassandra(keyspace='musicnet_metadata', table='song')
	
	print("Saved song data")
	time.sleep(1)
	
	#keysRDD.saveToCassandra(keyspace='musicnet_metadata', table='songkey')
	
	print("Saved key data")
	time.sleep(1)
	
	#chordsRDD.saveToCassandra(keyspace='musicnet_metadata', table='chord')
	
	print("Saved chord data")
	time.sleep(1)
		
	#meta = meta.filter(meta.composer == "Bach")
	
	metaknight = meta.map(lambda row: {'id': row.ID,
                                                  'composer': row.composer,
                                                  'ensemble': row.ensemble,
                                                  'source': row.source,
                                                  'transcriber': row.transcriber,
                                                  'catalog_name': row.catalog_name,
                                                  'seconds': row.seconds,
						  'movement': row.movement,
                                                  'composition': row.composition}).collect()
                                                  
	#~ for k in metaknight:
		#~ for kk in k:
			#~ print(kk + " ", k[kk])
			#~ time.sleep(1)

	sc.parallelize(metaknight).saveToCassandra(keyspace='musicnet_metadata', table='metadata')
	
	print("Saved metadata")
	
	elapsed = time.time() - startTime
	
	print("Total time: " + str(elapsed))



