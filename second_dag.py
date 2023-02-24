#!/usr/bin/env python
# coding: utf-8

# ## Airflow для решения аналитических задач.
# ### Код возвращает DAG, который находит ответы на вопросы
# 
# #### Файл, используемый для анализа - vgsales.csv
# #### Анализируемый год - 2015
# 
# Вопросы:
# * Какая игра была самой продаваемой в этом году во всем мире?
# * Игры какого жанра были самыми продаваемыми в Европе?
# * На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
# * У какого издателя самые высокие средние продажи в Японии?
# * Сколько игр продались лучше в Европе, чем в Японии?
# 

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[2]:


# path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
path = 'vgsales.csv'


# In[3]:


vgsales = pd.read_csv(path)


# In[4]:


year = 1994 + hash('i-fomina-27') % 23


# In[5]:


vgsales_2015 = vgsales.query(f'Year == {year}').reset_index().drop(columns='index', axis=1)


# In[ ]:





# ### Какая игра была самой продаваемой в 2015 году во всем мире?

# In[19]:


popular_game = vgsales_2015.groupby('Name', as_index=False)                             .agg({'Global_Sales': 'sum'})                            .sort_values('Global_Sales', ascending=False)                             .head(1).reset_index().drop(columns='index', axis=1)


# In[25]:


print(f'Самой продаваемой игрой во всем мире в 2015 году была игра "{popular_game.Name[0]}".')


# In[ ]:





# ### Игры какого жанра были самыми продаваемыми в Европе?

# In[38]:


europe_genres = vgsales_2015.groupby('Genre', as_index=False)                             .agg({'EU_Sales': 'sum'})                             .sort_values('EU_Sales', ascending=False)
                            .head(1).reset_index().drop(columns='index', axis=1)


# In[41]:


print(f'В 2015 году самыми продаваемыми были игры жанра "{europe_genres.Genre[0]}".')


# ### На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?

# In[49]:


na_platform = vgsales_2015.query('NA_Sales > 1')                           .groupby('Platform', as_index=False)                           .agg({'NA_Sales': 'count'})                           .sort_values('NA_Sales', ascending=False)                           .head(1).reset_index().drop(columns='index', axis=1)


# In[59]:


print(f'В Северной Америке в 2015 году на платформе "{na_platform.Platform[0]}" было больше всего игр, которые продались более чем миллионным тиражом.')


# ### У какого издателя самые высокие средние продажи в Японии?

# In[55]:


jp_avg_sales = vgsales_2015.groupby('Publisher', as_index=False)                            .agg({'JP_Sales': 'mean'})                            .sort_values('JP_Sales', ascending=False)                            .head(1).reset_index().drop(columns='index', axis=1)


# In[58]:


print(f'В Японии в 2015 году самые высокие средние продажи были у издателя "{jp_avg_sales.Publisher[0]}".')


# ### Сколько игр продались лучше в Европе, чем в Японии?

# In[67]:


eu_better_jp = vgsales_2015.groupby('Name', as_index=False)                            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})                            .sort_values('EU_Sales', ascending=False)                            .query('EU_Sales > JP_Sales')


# In[70]:


eu_better_jp.shape[0]


# In[71]:


print(f'В 2015 году {eu_better_jp.shape[0]} игр продались в Европе лучше, чем в Японии.')


# In[ ]:





# In[ ]:


import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash('i-fomina-27') % 23
default_args = {'owner': 'a.batalov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 2, 24)}


@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def fomina_vgsales():
    @task(retries=3)
    def get_data():
        vgsales_2015 = vgsales.query(f'Year == {year}').reset_index().drop(columns='index', axis=1)
        return vgsales_2015

    @task(retries=3)
    def get_popular_game(vgsales_2015):
        popular_game = vgsales_2015.groupby('Name', as_index=False)                                    .agg({'Global_Sales': 'sum'})                                   .sort_values('Global_Sales', ascending=False)                                    .head(1).reset_index().drop(columns='index', axis=1)
        return popular_game

    @task(retries=3)
    def get_europe_genres(vgsales_2015):
        europe_genres = vgsales_2015.groupby('Genre', as_index=False)                                     .agg({'EU_Sales': 'sum'})                                     .sort_values('EU_Sales', ascending=False)                                     .head(1).reset_index().drop(columns='index', axis=1)
        return europe_genres

    @task(retries=3)
    def get_na_platform(vgsales_2015):
        na_platform = vgsales_2015.query('NA_Sales > 1')                           .groupby('Platform', as_index=False)                           .agg({'NA_Sales': 'count'})                           .sort_values('NA_Sales', ascending=False)                           .head(1).reset_index().drop(columns='index', axis=1)
        return na_platform

    @task(retries=3)
    def get_jp_avg_sales(vgsales_2015):
        jp_avg_sales = vgsales_2015.groupby('Publisher', as_index=False)                            .agg({'JP_Sales': 'mean'})                            .sort_values('JP_Sales', ascending=False)                            .head(1).reset_index().drop(columns='index', axis=1)
        return jp_avg_sales

    
    @task(retries=3)
    def get_eu_better_jp(vgsales_2015):
        eu_better_jp = vgsales_2015.groupby('Name', as_index=False)                            .agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})                            .sort_values('EU_Sales', ascending=False)                            .query('EU_Sales > JP_Sales')
        return eu_better_jp

    @task(retries=3)
    def print_data(a, b, c, d, e):
        print(f'Самой продаваемой игрой во всем мире в 2015 году была игра "{popular_game.Name[0]}".')
        print(f'\nВ 2015 году самыми продаваемыми были игры жанра "{europe_genres.Genre[0]}".')
        print(f'\nВ Северной Америке в 2015 году на платформе "{na_platform.Platform[0]}" было больше всего игр, которые продались более чем миллионным тиражом.')
        print(f'\nВ Японии в 2015 году самые высокие средние продажи были у издателя "{jp_avg_sales.Publisher[0]}".')
        print(f'\nВ 2015 году {eu_better_jp.shape[0]} игр продались в Европе лучше, чем в Японии.')
        

    vgsales_2015 = get_data()
    popular_game = get_popular_game(vgsales_2015)
    europe_genre = get_europe_genres(vgsales_2015)
    na_platform  = get_na_platform(vgsales_2015)
    jp_avg_sales = get_jp_avg_sales(vgsales_2015)
    eu_better_jp = get_eu_better_jp(vgsales_2015)

    print_data(popular_game, europe_genre, na_platform, jp_avg_sales, eu_better_jp)
    
my_dag = fomina_vgsales()


# In[ ]:





# In[ ]:





# In[ ]:




