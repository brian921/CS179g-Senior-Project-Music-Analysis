import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel

class Song(DjangoCassandraModel):
	song_id = columns.Text(primary_key= True,required=False)
	start_time = columns.Text(required=False)
	end_time = columns.Text(required=False)
	instrument_id = columns.Text(required=False)
	measure = columns.Text(required=False)
	beat = columns.Text(required=False)
	note_value = columns.Text(required=False)
	song_id = columns.Text(required=False)
# Create your models here.
