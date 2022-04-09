from sqlite3 import connect
import time

create_table_track = '''
CREATE TABLE tracks(
IdArtist VARCHAR(225) PRIMARY KEY,
IdTrack VARCHAR(225),
ArtistName VARCHAR(225),
Title VARCHAR(225),
FOREIGN KEY(IdTrack) REFERENCES samples(IdTrack)
)
'''

create_table_samples = '''
CREATE TABLE samples(
Id INT PRIMARY KEY,
UserId VARCHAR(255),
IdTrack VARCHAR(225),
ListeningDate VARCHAR(225),
FOREIGN KEY(IdTrack) REFERENCES tracks(IdTrack)
)
'''


def main():
    trackPath = input("Podaj ścieżkę do pliku 'unique_tracks' =>  ")
    samplesPath = input("Podaj ścieżkę do pliku 'triplets_sample_20p' =>  ")
    databasePath = input("Gdzie utworzyć baze danych (Podaj scieżkę do folderu) =>  ")

    if databasePath[-1] != "\\":
        databasePath = databasePath + "\\"

    start = time.time()    
    Insert_into_database(trackPath,samplesPath,databasePath)
    end = time.time()
    print('Zadanie Wykonane w ' + str(end-start) + ' sekund')

def Insert_into_database(trackPath,samplesPath,databasePath):
   
    with connect(databasePath + 'ETL.db') as db_connector:
        db_cursor=db_connector.cursor()
        db_cursor.execute('DROP TABLE IF EXISTS tracks')
        db_cursor.execute('DROP TABLE IF EXISTS samples')
        db_cursor.execute(create_table_track)
        db_cursor.execute(create_table_samples)

        with open(trackPath,'r',encoding='ansi' ) as f:
            for line in f:
                
                    line=line.strip() 
                    track_data = line.split('<SEP>') 
                    db_cursor.execute('INSERT INTO  tracks VALUES (?,?,?,?)',track_data)
        print("Track table filed with data")

        with open(samplesPath,'r',encoding='ansi' ) as f:
            counter = 1
            for line in f:
                    line=line.strip() 
                    sample_data = line.split('<SEP>') 
                    sample_data.insert(0,counter)
                    db_cursor.execute('INSERT INTO  samples VALUES (?,?,?,?)',sample_data)
                    counter+=1
        print("Samples table filed with data")

     # Program musi wyświetlić nazwę artysty z największą liczbą odsłuchań oraz liczbę tych odsłuchań.
    mostPoppularArtist = """
            SELECT  t.ArtistName, count(l.IdTrack) FROM
                samples as l, tracks as t
                WHERE l.IdTrack=t.IdTrack
                GROUP BY t.ArtistName
                ORDER BY COUNT(t.ArtistName) DESC
                LIMIT 1;
            """
    artist = db_cursor.execute(mostPoppularArtist)
    for row in artist:
        print("Najpopularniejszy artysta to :" + row[0] + " Liczba odtworzeń: " + str(row[1]))   

    #Program musi wyświetlić tytuły 5 utworów z największą liczbą odsłuchań oraz liczbę tych odsłuchań.
    listOfPopularTrack = """
            SELECT  t.Title,count(l.IdTrack)  FROM
            samples as l, tracks as t
            WHERE l.IdTrack=t.IdTrack
            GROUP BY l.IdTrack
            ORDER BY COUNT(l.IdTrack) DESC
            LIMIT 5;
            """ 
    tracks = db_cursor.execute(listOfPopularTrack)
    print("Lista TOP 5 najpopularniejszych utworuów")
    for row in tracks:
        print("Nazwa utworu :" + row[0] + " Liczba odtworzeń: " + str(row[1])) 

main()
