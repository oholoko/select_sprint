import boto3
import json
import os
import sqlalchemy
import pandas as pd


class ETL():
	__json = {'trip2012':'data-sample_data-nyctaxi-trips-2012-json_corrigido.json',
	'trip2011':'data-sample_data-nyctaxi-trips-2011-json_corrigido.json',
	'trip2010':'data-sample_data-nyctaxi-trips-2010-json_corrigido.json',
	'trip2009':'data-sample_data-nyctaxi-trips-2009-json_corrigido.json'
	}

	__csv = {'vendor':'data-vendor_lookup-csv.csv',
	'payment':'data-payment_lookup-csv.csv'}


	__bucket = 'data-sprints-eng-test'

	__http = 'https://s3.amazonaws.com'

	
	    
	#__DBHOST=os.getenv('DBHOST')
	#__DBPORT=os.getenv('DBPORT')
	#__DBUSER=os.getenv('DBUSER')
	#__DBPASS=os.getenv('DBPASS')
	
	content = {}

	def __init__(self,**options):
		if(options.get('json') != None):
			self.__json = options.get('json')

		if(options.get('csv') != None):
			self.__csv = options.get('csv')

		if(options.get('bucket') != None):
			self.__bucket = options.get('bucket')

		if(options.get('awsKey') != None):
			__client = boto3.client(
		    	's3',
		    	aws_access_key_id= options.get('awsKey')[0],
		    	aws_secret_access_key= options.get('awsKey')[1]
			)

		#self.engine = sqlalchemy.create_engine('postgres://{}:{}@{}:{}'.format(
		#		self.__DBUSER,self.__DBPASS,self.__DBHOST,self.__DBPORT))
		#if(options.get('create') != None):
		#	escaped_sql = sqlalchemy.text(options.get('create'))
		#else:
		#	file = open('create.sql')
		#	escaped_sql = sqlalchemy.text(file.read())
		
		#self.engine.execute(escaped_sql)

	def load_database(self):
		for each in self.__csv:
			self.content[each] = self.__client.get_object(Bucket=self.__bucket,
				Key=self.__csv[each])['Body']
			if each == 'payment':
				aux = pd.read_csv(self.content[each],skiprows=1)
			else:
				aux = pd.read_csv(self.content[each])
			aux.to_csv(each+'.csv',index=False)

			
		T = {'vendor_id': [],
			'passenger_count':[],
			'pickup_datetime': [], 
			'pickup_longitude': [], 
			'pickup_latitude': [],
			'dropoff_datetime':[],
			'dropoff_longitude': [], 
			'dropoff_latitude': [],
			'payment_type': [],
			'trip_distance': [],
			'fare_amount': [],
			'surcharge': [], 
			'tip_amount': [],
			'tolls_amount': [],
			'total_amount': []
			}
		i = 0
		#META_DATA = sqlalchemy.MetaData(bind=self.engine, reflect=True)
		#table = META_DATA.tables['nyctaxy']
		for each in self.__json:
			self.content[each] = self.__client.get_object(Bucket=self.__bucket,
				Key=self.__json[each])['Body']
			for each1 in self.content[each].iter_lines():
				i+=1
				aux = json.loads(each1.decode())
				for each2 in T:
					T[each2].append(aux[each2])
				if(i%100001 == 0):
					tabela = pd.DataFrame(T)
					tabela.to_csv(each+str(int(i/100001))+'.csv',index=False)
					for each2 in T:
						T[each2] =[]
			if i%100001 > 0:
				tabela = pd.DataFrame(T)
				tabela.to_csv(each+str(int(i/100001)+1)+'.csv',index=False)
				for each2 in T:
					T[each2] =[]
			i = 0