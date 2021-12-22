import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt


def structured():
    conn = mysql.connector.connect(user='zipcoder3', password='zipcode0', host='localhost',
                                   database='netflix_mysql_database', use_pure=True)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM netflix_mysql_table;")
    table = cursor.fetchall()  # get all records
    # print(cursor.rowcount)
    # data = pd.read_sql(table, conn)
    data = pd.DataFrame(table, columns=['show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added',
                                        'release_year', 'rating', 'duration', 'listed_in', 'description'])

    movies_only = data[data.type == 'Movie']

    movies_nan_drop = movies_only.dropna(subset=['country'])

    single_countries = movies_nan_drop[movies_nan_drop['country'].str.contains(",") == False]

    new_data = single_countries[single_countries['listed_in'].str.contains(",") == False]

    new_data_bar = new_data[['country', 'listed_in']].groupby('listed_in').count()

    new_index = ['Stand-Up Comedy', 'Documentaries', 'Children & Family Movies', 'Dramas', 'Comedies',
                 'Action & Adventure', 'Thrillers', 'Horror Movies']

    new_data_reindexed = new_data_bar.reindex(new_index)

    new_data_reindexed = new_data_reindexed.rename(columns={'country': "count"})

    plt.style.use('ggplot')
    new_data_reindexed.plot.bar()
    plt.tight_layout()
    plt.show()
    conn.commit()
    conn.close()
