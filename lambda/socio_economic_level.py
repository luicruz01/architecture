import requests
import pandas as pd

"""
We can get the socio economi level from a zone or a grid from deiverse ways, 
One it's obtained the INEGI data from the DENUE and the last population census
also we need to get the last destination origin survey.
The other one is buy the data from a third party, in any case we need at last the
cordinates from the property ans the radius that represents the property size.
"""

def get_socio_economic_level_from_third(lat, lon, radius):
	URL_THIRD = "URL_THIRD?lat={lat}&lon={lon}&radius={radius}"
	return requests.get(URL_THIRD.format(lat=lat, lon=lon, radius=radius))


def get_socio_economic_from_scratch(lat, lon, radius):
	# Get the DENUE data
	local_bussines = get_bussines(lat, lonm radius)
	origin, destination = get_origin_destiny(lat, lon, radius)
	bussines_origin = [get_bussines(x) for x in origin if x is not None]
	bussines_destination = [get_bussines(x) for x in destination if x is not None]
	socio_economic_level = 0
	for bo in bussines_origin:
		socio_economic_level += bo
	return (socio_economic_level + local_bussines) / (len(local_bussines+1))
	

def get_bussines(lat, lon, radius)
	query = """
		SELECT business_indicator FROM denue
		WHERE ST_DWITHIN(geom_way, ST_GEOMFROMEWKT('SRID=4326;POINT({lon} {lat})'), {radius})
	"""
	business = conn.execute(query=query.format(lat=lat, lon=lon, radius=radius+0.00015)) # the 0.0005 will 150 meters to the original radius
	df = pd.DataFrame(business)
	business_average = df.groupby('weight').mean()
	return business_average


def get_origin(lat, lon, radius):
	#Get destination origin survey data.
	query = """
		SELECT ST_X(geom_way_origin), ST_Y(geom_way_origin) FROM origin_destiny
		WHERE ST_DWITHIN(geom_way_destiny, ST_GEOMFROMEWKT('SRID=4326;POINT({lon} {lat})'), {radius})
	"""
	origins = conn.execute(query=query.format(lat=lat, lon=lon, radius=radius+0.00015))
	return origins