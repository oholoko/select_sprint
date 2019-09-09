import boto3
import gmplot
import json
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

class analysis():
	tabela = {}
	def __init__(self,mode,**options):
		self.mode = mode
		if mode == 'csv':
			for each in options:
				self.tabela[each] = options.get(each)
		
	def get_average(self):
		if self.mode == 'csv':
			tempdist = 0
			temptotal = 0
			for each in self.tabela['trips']:
				aux = pd.read_csv(each)
				aux = aux[aux['passenger_count'] <= 2]
				tempdist += aux['trip_distance'].sum()
				temptotal += aux['trip_distance'].count()
			avg_dist = tempdist/temptotal
			return avg_dist
		else:
			return False


	def get_vendors(self):
		if self.mode == 'csv':
			T = []
			for each in self.tabela['trips']:	
				aux = pd.read_csv(each)
				aux = aux[['vendor_id','total_amount']]
				if len(T) == 0:
					T = aux.groupby('vendor_id').sum().reset_index()
				else:
					aux = pd.concat([aux,T])
					T = aux.groupby('vendor_id').sum().reset_index()
			vendors = pd.read_csv(self.tabela['vendor'])
			vendors = T.merge(vendors,on='vendor_id')
			return vendors


	def get_cash_histogram(self):
		plt.close('all')
		if self.mode == 'csv':
			T1 = []
			payment = pd.read_csv(self.tabela['payment'])
			for each in self.tabela['trips']:
				aux = pd.read_csv(each)
				aux = aux.merge(payment,on='payment_type')
				aux = aux[['pickup_datetime','dropoff_datetime',
				'payment_lookup']]
				aux.loc[:,'pickup_datetime'] = pd.to_datetime(
					aux['pickup_datetime'])
				aux.loc[:,'dropoff_datetime'] = pd.to_datetime(
					aux['dropoff_datetime'])
				aux.loc[:,'pickup_datetime'] = aux['pickup_datetime'].apply(
					lambda x: int(str(x.year) + ('0'+str(x.month) if x.month < 10 else str(x.month))))
				aux.loc[:,'dropoff_datetime'] = aux['dropoff_datetime'].apply(
					lambda x: int(str(x.year) + ('0'+str(x.month) if x.month < 10 else str(x.month))))
				aux = aux[aux['payment_lookup'] == 'Cash']
				aux = aux[['pickup_datetime','dropoff_datetime']]
				aux['count'] = 1
				if len(T1) == 0:
					T1 = aux[['pickup_datetime','count']].groupby(
						'pickup_datetime').sum().reset_index()
					T2 = aux[['dropoff_datetime','count']].groupby(
						'dropoff_datetime').sum().reset_index()
				else:
					T1 = pd.concat([aux[['pickup_datetime','count']].groupby(
						'pickup_datetime').sum().reset_index(),T1])
					T2 = pd.concat([aux[['dropoff_datetime','count']].groupby(
						'dropoff_datetime').sum().reset_index(),T2])
		T1 = T1.groupby('pickup_datetime').sum().reset_index()
		T1.plot(kind='bar',x='pickup_datetime',y='count')
		plt.show()
		T2 = T2.groupby('dropoff_datetime').sum().reset_index()
		T2.plot(kind='bar',x='dropoff_datetime',y='count')
		plt.show()


	def get_tips_series(self):
		if self.mode == 'csv':
			T1 = []
			for each in self.tabela['trips']:
				aux = pd.read_csv(each)
				aux = aux[['pickup_datetime','dropoff_datetime',
				'tip_amount']]
				aux.loc[:,'pickup_datetime'] = pd.to_datetime(
					aux['pickup_datetime'])
				aux.loc[:,'dropoff_datetime'] = pd.to_datetime(
					aux['dropoff_datetime'])
				aux.loc[:,'pickup_datetime'] = aux['pickup_datetime'].apply(
					lambda x: int(str(x.year) + 
						('0'+str(x.month) if x.month < 10 else str(x.month))+
						('0'+str(x.day) if x.day < 10 else str(x.day))))
				aux.loc[:,'dropoff_datetime'] = aux['dropoff_datetime'].apply(
					lambda x: int(str(x.year) + 
						('0'+str(x.month) if x.month < 10 else str(x.month))+
						('0'+str(x.day) if x.day < 10 else str(x.day))))
				aux = aux[(aux['pickup_datetime'] >= 20120800) | 
							(aux['dropoff_datetime'] >= 20120800)]
				if len(T1) == 0:
					T1 = aux[['pickup_datetime','tip_amount']].groupby(
						'pickup_datetime').sum().reset_index()
					T2 = aux[['dropoff_datetime','tip_amount']].groupby(
						'dropoff_datetime').sum().reset_index()
				else:
					T1 = pd.concat([aux[['pickup_datetime','tip_amount']].groupby(
						'pickup_datetime').sum().reset_index(),T1])
					T2 = pd.concat([aux[['dropoff_datetime','tip_amount']].groupby(
						'dropoff_datetime').sum().reset_index(),T2])
		T1.loc[:,'pickup_datetime'] = T1['pickup_datetime'].astype(str)
		T1.loc[:,'pickup_datetime'] = pd.to_datetime(T1['pickup_datetime'],
									format='%Y%m%d', errors='ignore')
		T1 = T1.groupby('pickup_datetime').sum().reset_index()
		T1.plot(x='pickup_datetime',y='tip_amount')
		plt.show()

		T2.loc[:,'dropoff_datetime'] = T2['dropoff_datetime'].astype(str)
		T2.loc[:,'dropoff_datetime'] = pd.to_datetime(T2['dropoff_datetime'],
									format='%Y%m%d', errors='ignore')
		T2 = T2.groupby('dropoff_datetime').sum().reset_index()
		T2.plot(x='dropoff_datetime',y='tip_amount')
		plt.show()

	def get_average_Weekend(self):
		if self.mode == 'csv':
			temptotal = 0
			numdias = 0
			for each in self.tabela['trips']:
				aux = pd.read_csv(each)
				aux.loc[:,'pickup_datetime'] = pd.to_datetime(aux.loc[:,'pickup_datetime'])	
				aux.loc[:,'dropoff_datetime'] = pd.to_datetime(aux.loc[:,'dropoff_datetime'])
				aux = aux[['pickup_datetime','dropoff_datetime']]
				aux['tempo'] = (aux['dropoff_datetime']- aux['pickup_datetime']).dt.seconds
				aux['dia_entrada'] = aux['pickup_datetime'].apply(lambda x: x.weekday())
				aux['dia_saida'] = aux['dropoff_datetime'].apply(lambda x: x.weekday())
				aux = aux[(aux['dia_entrada'] == 5) | (aux['dia_entrada'] == 6) |
							(aux['dia_saida'] == 5) | (aux['dia_saida'] == 6)]
				temptotal += aux['tempo'].sum()
				numdias += aux['tempo'].count()
			self.avg_time = temptotal/numdias
			return self.avg_time

	def show_start_endpoint(self):
		if self.mode == 'csv':
			data = []
			for each in self.tabela['trips']:
				aux = pd.read_csv(each)
				aux = aux[['pickup_longitude','pickup_latitude',	
					'dropoff_longitude', 'dropoff_latitude',
					'pickup_datetime','dropoff_datetime']]
				aux.loc[:,'pickup_datetime'] = pd.to_datetime(
					aux['pickup_datetime'])
				aux.loc[:,'dropoff_datetime'] = pd.to_datetime(
					aux['dropoff_datetime'])
				aux['pickup_datetime'] = aux['pickup_datetime'].apply(lambda x: x.year)
				aux['dropoff_datetime'] = aux['dropoff_datetime'].apply(lambda x: x.year)
				aux = aux[(aux['pickup_datetime'] == 2010) | (aux['dropoff_datetime'] == 2010)]
				if len(data) == 0:
					data = aux[['dropoff_longitude', 'dropoff_latitude',
							'pickup_longitude','pickup_latitude']]
				else:
					data = pd.concat([aux[['dropoff_longitude', 'dropoff_latitude',
							'pickup_longitude','pickup_latitude']],data])
		latitudes = data["pickup_latitude"]
		avgLat = data["pickup_latitude"].mean()
		longitudes = data["pickup_longitude"]
		avgLong = data["pickup_longitude"].mean()

		gmap = gmplot.GoogleMapPlotter(avgLat, avgLong, 10)
		gmap.heatmap(latitudes, longitudes)
		gmap.draw("pegadaHeat.html")

		latitudes = data["dropoff_latitude"]
		avgLat = data["dropoff_latitude"].mean()
		longitudes = data["dropoff_longitude"]
		avgLong = data["dropoff_latitude"].mean()

		gmap = gmplot.GoogleMapPlotter(avgLat, avgLong, 10)
		gmap.heatmap(latitudes, longitudes)
		gmap.draw("saidaHeat.html")





#aux = analysis(mode='csv',trips=trip)
#aux.get_average()
#aux.get_vendors()
#aux.get_cash_histogram()
#aux.get_tips_series()
#aux.get_average_Weekend()
#aux.show_start_endpoint()