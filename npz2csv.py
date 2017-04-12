import numpy as np
a = np.load('./musicnet.npz')

first_song_id = 1727
final_song_id = 2678 #last song id is 2678

for x in range(first_song_id, final_song_id):
        try:
                np.savetxt('./csvdir/' + str(x) + '.csv', a[str(x)], delimiter=",", fmt="%s")
                #for interval_obj in a[str(x)][1]:
                #       print interval_obj

        except:
                pass
