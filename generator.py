from tracemalloc import start
from matplotlib.pyplot import cla
import pandas as pd
import requests
import functools
from functools import cache
from bs4 import BeautifulSoup
import names
import time
import random
from datetime import timedelta
import datetime
from scipy import rand
import numpy as np
import math
import string
from excel_gen import excel_generator
import threading 


BIRTH_BEGIN = datetime.date(1965,1,1)
BIRTH_END = datetime.date(2022,3,10)

EXP_BEGIN = datetime.date(2022,4,1)
EXP_END = datetime.date(2035,1,1)

FLIGHTS_BEGIN = datetime.datetime.strptime('1/1/1990 1:30 PM', '%m/%d/%Y %I:%M %p')
FLIGHTS_END = datetime.datetime.strptime('3/19/2022 4:50 AM', '%m/%d/%Y %I:%M %p')


AIRLINES = ['Air France','WizzAir','RyanAir','Etihad Airlines','United Airlines']
WEIGHTS = [0.4,0.25,0.05,0.1,0.2]

AVG_PEOPLE_PER_FLIGHT = 100

random.seed(69)

class data_generator:
    '''Generator creates 1 000 000 entires for tables in AFIS database. For each table data_generator
    creates csv file filled with generated entries. Moreover data for QuestionnaireData is generated and returned
    in form of .xlsx file
    Data about the destinations is scrapped from https://www.flightsfrom.com/CDG/destinations for Charles de Gaule
    airport'''

    _destinations : pd.DataFrame
    _passport_details: pd.DataFrame
    _flight_information: pd.DataFrame
    _flight_attendant: pd.DataFrame
    _tickets: pd.DataFrame 
    _questionnaireData: pd.DataFrame

    _flight_time: dict

    def __init__(self):
        self._destinations = self._init_dest()
        self._passport_details = self._init_pass()
        self._flight_information = self._init_fi()
        self._flight_attendant = self._init_fa()
        self._tickets = self._init_ticket()
        
        print('done')
        excel = excel_generator(self._tickets)
        self._questionnaireData = excel.generate_df()
        print('excel done')
        self._process_dataframes()

    @property
    def destination(self):
        return self._destinations

    @property
    def passport_details(self):
        return self._passport_details

    @property
    def flight_information(self):
        return self._flight_information

    @property
    def flight_attendant(self):
        return self._flight_attendant

    @property
    def tickets(self):
        return self._tickets
        

    def _init_dest(self):
        data = self._scrap_destinations()
        return pd.DataFrame(data=data, columns=['ID','City','Country']).set_index('ID')

    def _init_pass(self):
        data = self._generate_pass()
        return pd.DataFrame(data=data).set_index('ID')

    def _init_fi(self):
        data = self._generate_flights()
        return pd.DataFrame(data=data).set_index('ID')

    @cache
    def _init_fa(self):
        '''
        fk_p - passengers' ids
        fk_fi - each flight's identification number
        '''
        fk_p = []
        fk_fi = []

        keys = ['Passport_Details_ID','Flight_Information_ID']

        data_dict = {}

        for i in range(len(self._passport_details)):
            if i%10==0:
                fk_p.append(random.randint(1,len(self._passport_details)))
            else:
                fk_p.append(i)
            fk_fi.append(random.randint(1,len(self._flight_information)))

        data_dict = self._apply_keys(keys, fk_p, fk_fi)

        return pd.DataFrame(data = data_dict).set_index(keys)

    def _init_ticket(self):
        data = self._generate_tickets()
        return pd.DataFrame(data = data).set_index('TicketID')

    def _process_dataframes(self):
        self._destinations.to_csv('destinations.csv')
        self._passport_details.to_csv('passport_details.csv')
        self._flight_information.to_csv('flight_information.csv')
        self._flight_attendant.to_csv('flight_attendant.csv')
        self._tickets.to_csv("tickets.csv")
        self._questionnaireData.to_excel("questionnaireData.xlsx", engine='xlsxwriter')
    
    @cache
    def _generate_tickets(self):
        '''
        ticket_id - ticket identification number
        fk_fa1, fk_fa2 - combination which acts as a foreign key from _flight_attendant, divided into 2 columns
                         for ease of creating composite foreign key in database as well as for sorting values
                         in this function for distributing seats and classes
        class_ - randomly selected class 
        seat - randomly selected seat 
        '''

        data_dict = {}

        df = self._flight_attendant.reset_index()
        
        l = ['a','b','c','d','e','f']   


        class_ = np.random.choice(a=["First Class", "Business Class", "Economy Class"],
                                  p = [0.6,0.3,0.1], size = len(df))
        
        seats = [l[i%6] for i in range(len(df))]

        fk_fa1 = df['Passport_Details_ID']
        fk_fa2 = df['Flight_Information_ID']

        def Convert(string):
            list1=[]
            list1[:0]=string
            return list1

        random_ascii = np.random.choice(Convert(string.ascii_letters),size=len(df))
    
        ticket = [elem[0] + seats[i] + str(fk_fa1[i]) + str(fk_fa2[i]) + str(random_ascii[i]) for i,elem in enumerate(class_)]

        keys = ['TicketID','Flight Attendant','Flight Information','Class','Seat']    
        data_dict = self._apply_keys(keys, ticket, fk_fa1, fk_fa2, class_, seats)

        return data_dict

    @cache
    def _generate_flights(self):
        '''
        ids - flight identifiaction number
        fk_dest - id randomly selected from _flight_time
        airline - airline dealing with given flight
        dt_a - date and time of arrival at destination
        dt_d - dt_a + flight time from _flight_time for given id
        '''

        ids = np.arange(0,1_000_000)

        #generating random dest id
        f_keys = list(self._flight_time.keys())
        fk_dest = np.random.choice(f_keys, size=1_000_000)

        #generating random airlines 
        airline = np.random.choice(a=AIRLINES, p=WEIGHTS, size=1_000_000)

        
        #dates of departure and arrival
        dt_d = [self._random_datetime(FLIGHTS_BEGIN,FLIGHTS_END) for x in range(1_000_000)]
        dt_a = [dt_d[x] + datetime.timedelta(hours = self._flight_time[fk_dest[x]].hour, minutes=self._flight_time[fk_dest[x]].minute) for x in range(1_000_000)]
        
        keys = ['ID','Destination','Airline','Date and time of arrival','Date and time of departure']
        data_dict = self._apply_keys(keys, ids, fk_dest, airline, dt_a, dt_d)

        return data_dict

    @cache
    def _generate_pass(self):
        '''
        ids - identification numbers
        names - names of passagers
        surnames - surnames
        dob - date of birth
        pob - place of birth
        por - place of residence
        nat - nationality
        exp - passport expriry date 
        email - passenger's email

        data_dict - dictionnaire containing all generated data
        '''

        cities = self._generate_cities()

        #generating ids
        ids = np.arange(0,1_000_000)

        #generating names
        with open('usf.txt','r') as file:
            names_ = [file.readline() for x in range(1_000_000)]

        with open('uss.txt','r') as file:
            surnames = [file.readline() for x in range(1_000_000)]

        names_ = [elem.strip('\n') for elem in names_]
        surnames = [elem.strip('\n') for elem in surnames]

        #generating dates
        dob = [self._random_date(BIRTH_BEGIN, BIRTH_END).strftime("%D") for x in range(1_000_000)]
        exp = [self._random_date(EXP_BEGIN, EXP_END).strftime("%D") for x in range(1_000_000)]

        #generating cities
        pob = np.random.choice(cities, size=1_000_000)
        por = np.random.choice(cities, size=1_000_000)

        #rest
        nat = [pob[x].split(", ")[1] for x in range(1_000_000)]
        email = [names_[x] + '.' + surnames[x] + "@mail.com" for x in range(1_000_000)]

        data_dict = {}
        keys = ['ID','Name','Surname','Day of birth', 'Place of birth', 'Place of residence', 'Nationality', 
        'Document expiration date', 'Contact email']

        data_dict = self._apply_keys(keys,ids, names_, surnames, dob, pob, por, nat, exp, email)

        return data_dict

    def _apply_keys(self,keys: list, *args):
        dict_ = {}
        for i,arg in enumerate(args):
            dict_[keys[i]] = arg

        return dict_

    def _generate_cities(self, URL = "https://data.mongabay.com/cities_pop_01.htm")->list:
        r = requests.get(URL)
        data = []
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'lxml')
            table1 = soup.find('table', class_='boldtable')
            for j in table1.find_all('tr')[1:]:
                row_data = j.find_all('td')
                row = [i.text for i in row_data]
                data.append(row[0])
        data.remove('CityCountry')
        return data

    def _random_date(self,start:datetime, end:datetime):
        """
        This function will return a random datetime between two datetime 
        objects.
        """
        time_between_dates = end - start
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start + datetime.timedelta(days=random_number_of_days)

        return random_date

    def _random_datetime(self,start,end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)


    def _scrap_destinations(self, URL = "https://www.flightsfrom.com/CDG/destinations"):
        r = requests.get(URL)
        data = []
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'lxml')
            data_dest = soup.find_all('div', class_= "airport-content-destination-list-name")
            data_times = soup.find_all('div', class_= "airport-content-destination-list-time")
            for dest in data_dest:
                l = self._clean_destination(dest.text)
                l[1], l[0] = l[0], l[1] #swap elements so that they are in correct order
                data.append(l)

        #_scrap_destinations also triggers function that creates dictionary which combines 
        # flight ID's and their corresponding flight times
        self._flight_time = self._flight_time_init(data, data_times) 

        return data

    def _clean_destination(self, str_):

        def filter_elements(char):
            elems = ['','\t','\n','\r']
            return False if char in elems else True

        def get_null_id(l:list):
            for i,elem in enumerate(l):
                if elem == '':
                    return i

        def combine_elements(l:list, id:int):
            if(id > 1):
                l[0:id] = [' '.join(l[0:id])]

            id_pos = get_null_id(l) + 1

            if(id_pos != len(l)-2):
                l[id_pos + 1:] = [' '.join(l[id_pos + 1:])]
            return l


        str_ = list(filter(filter_elements, str_))[:-6]
        str_ = ''.join(str_).split(' ')
        
        str_ = combine_elements(str_, get_null_id(str_))
        str_.remove('')
        return str_

    def _flight_time_init(self, data:list, data_times:list):
        ftd = {}
        keys = [d[0] for d in data]
        times = []
        for dt in data_times:
            str_ = dt.text
            str_ = str_.split()[2:]
            str_[0] = str_[0][:-1]
            str_[1] = str_[1][:-1]
            times.append(datetime.time(int(str_[0]),int(str_[1])))
        
        ftd = dict(zip(keys,times))

        return ftd
        

        
    