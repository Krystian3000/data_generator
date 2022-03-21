import pandas as pd
import numpy as np

np.random.seed(69)

class excel_generator:

    _Security_satisfaction : list
    _Information_satisfaction : list
    _Waiting_time_satifaction : list
    _Luggage_treatment_satisfaction : list
    _Check_in_satisfaction : list

    _ID : list
    _TicketsID : list

    def __init__(self, tickets : pd.DataFrame):
        
        size = int(len(tickets))

        self._Security_satisfaction = self._gen_ratings(size)
        self._Information_satisfaction = self._gen_ratings(size)
        self._Waiting_time_satifaction = self._gen_ratings(size)
        self._Luggage_treatment_satisfaction = self._gen_ratings(size)
        self._Check_in_satisfaction = self._gen_ratings(size)

        self._ID = list(range(size))
        self._TicketsID = np.random.choice(tickets.index.values.tolist(), size=size)

            
    def _gen_ratings(self, size):
        list_ = np.random.normal(loc = 3, scale = 1, size = size)
        list_[list_>5] = 5
        list_[list_<1] = 1

        list_ = [round(x) for x in list_]

        return list_

    def generate_df(self):
        df = pd.DataFrame({
            "ID":self._ID,
            "Security_satisfaction":self._Security_satisfaction,
            "Information_satisfaction" : self._Information_satisfaction,
            "Waiting_time_satifaction" : self._Waiting_time_satifaction,
            "Luggage_treatment_satisfaction" : self._Luggage_treatment_satisfaction,
            "Check_in_satisfaction" : self._Check_in_satisfaction,
            "Ticket ID" : self._TicketsID
        }).set_index('ID')
        
        df = df.astype({
            "Security_satisfaction": 'int',
            "Information_satisfaction" : 'int',
            "Waiting_time_satifaction" : 'int',
            "Luggage_treatment_satisfaction" : 'int',
            "Check_in_satisfaction" : 'int',
            "Ticket ID" : 'object'
        })

        return df

