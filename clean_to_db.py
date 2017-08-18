import xml.etree.cElementTree as ET
from collections import defaultdict
import pprint
import json
import re
import codecs

"""
This script clean the problematic entries and pass it on to a database. 
Make sure "mongod" instance is running in background as the generated 
data will be passed to the mongodb database.
"""


"""==========================================================================================
Functions used to clean various fields. 
Takes in data from various fields and returns cleaned values.
=========================================================================================="""
def clean_postcode(pincode): 
    try:
        # pincodes which match the re-expression is extracted, others are cleaned
        pincode_re=re.compile(r'^([0-9]{5})(?:-[0-9]{4})?$')
        m=pincode_re.match(pincode.strip("TN "))
        if m:
            good_pin=(m.groups()[0])
        return int(good_pin)
    except UnboundLocalError:
        # print("From clean_pincode, Bad postcode\t:",pincode)# Uncomment to print bad picodes
        pass

def clean_city(city):
    city_mapping={'nashville':'Nashville', 
                  'Murfreesboro, TN':'Murfreesboro',
                  'murfreesboro':'Murfreesboro',
                  'hermitage':'Hermitage',
                  'dickson':'Dickson',
                  'Thompsons Station':'Thompson\'s Station',
                  'Thompson""s Station':'Thompson\'s Station', 
                  'Antler, Tennessee':"Antler",
                  'Gallatin, TN':'Gallatin', 
                  'LaVergne':'La Vergne',
                  'Mount Joliet':'Mount Juliet',
                  'Normandy, TN':'Normandy'}
    if city in city_mapping:
        city=city_mapping[city]
    return city


def clean_phone(number):
    # returns phone numbers after cleaningin the formart: 1 (xxx)-xxx-xxxx, 
    # discarded numbers are printed
    try:
        phone_re=re.compile(r"^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$")
        m=phone_re.match(number.lstrip("+1- ").strip())
        phone=m.groups()
        # phone="1 ({0})-{1}-{2}".format(m.groups(0)[0],m.groups(0)[1], m.groups(0)[2])
        return phone
    except AttributeError:
#         print("From clean_phone, Bad number\t:",number) # Uncomment to print bad phone
        pass

def clean_streetnames(street):
    # street names in the mapping are cleaned
    # street names are passed without change if in the expected values or not in mapping
    street_name_re=re.compile(r"\b\S+\.?$", re.IGNORECASE)
    EXPECTED_STREET_NAMES = ["Avenue", "Court", "Lane", "Boulevard", "Drive", "Court",
                        "Place","Road","Parkway","Circle","South","North","Highway",
                         "Trail","Terrace","Square", "Pike","Alley","Street","Trace",
                         "Bypass","Way","Fork", "Plaza","Broadway", "Loop", "Cove",
                        "Flagpole","Foxborough", "Foxland", "Center", "Hollow","East",
                        "Heights","Landing", "Springs", "Hills", "Mission", "Pass",
                        "Vandy", "Glen","Padgett","Wynthrope"]
    street_mapping={'AVENUE':'Avenue', # mapping for the street names
                  "Ave":"Avenue",
                  "Blvd":"Boulevard",
                  "BLVD":"Boulevard",
                  "Cir":"Circle",
                  "Crt":"Court",
                  "Ct":"Court",
                  "Dr":"Drive",
                  "Hwy":"Highway",
                  "Hwy.":"Highway",
                  "Pk":"Park",
                  "Ln":"Lane",
                  "Pky":"Parkway",
                  "Pkwy":"Parkway",
                  "Pl":"Place",
                  "Rd":"Road",
                  "S":"South",
                  "St":"Street",
                  "St.":"Street",
                  "ave":"Avenue",
                  "avenue":"Avenue",
                  "hills":"Hills",
                  "pike":"Pike",
                  "st":"Street"}
                  
    try:
        m=street_name_re.search(street)
        if m and (m.group() not in EXPECTED_STREET_NAMES):
#             print(street_mapping[m.group()])
            new_street=street_name_re.sub(street_mapping[m.group()],street)
#             print("From clean_streetnames,\n Unclean street\t:",street,
#                   "\tClean street\t:",new_street)
            return new_street
        else:
            return street
    except (AttributeError, KeyError):
#         print("From clean_streetnames, Unclean street\t:",street)
        return street

"""==========================================================================================
=========================================================================================="""

"""
Cleaning data to pass to database
=================================
A general structure of data:
   {
    'address': {'neighbourhood': ...
                'country': 'USA', 
                'state': 'TN', 
                'street': 'Old Rocky Fork'},
     'created': {'changeset': '4470981',
                 'timestamp': '2010-04-19T18:58:26Z',
                 'uid': '270262',
                 'user': 'Ab Ye',
                 'version': '1'},    
     'e_id': '702177640',
     'name': 'Nolensville Ball Park',
     'pos': [35.9544391, -86.6665184],
     'type': 'node'
     'amenity': ""
     'ref':[345534,534534,534534] 
    } 
     # references are added from  <"nd"> for <"way"> && <"member"> for <"relation">

"""


def shape_data(element):
    if element.tag in ["relation", "way", "node"]:
        data={}
        data["type"]=element.tag
        data["e_id"]=element.attrib["id"]
        
        created={}
        for field in [ "version", "changeset", "timestamp", "user", "uid"]:
            created[field]=element.attrib[field]
        if created!={}:
            data["created"]=created
        
        if element.tag=="node": # add position as list : pos=[lat,lon]
            pos=[0,0]
            pos[0]=float(element.attrib["lat"])
            pos[1]=float(element.attrib["lon"])
            data["pos"]=pos
        
        address={} # initializing address field
        for elem in element.iter("tag"):
            if elem.attrib["k"]=="addr:postcode": #add cleaned postcode
                postcode=clean_postcode(elem.attrib["v"])
                if postcode:
                    address["postcode"]=postcode
            if elem.attrib["k"]=="addr:street": # add street after mapping
                if elem.attrib["v"]:
                    address["street"]=clean_streetnames(elem.attrib["v"])
            if elem.attrib["k"]=="addr:city": # add city after mapping
                if elem.attrib["v"]:
                    address["city"]=clean_city(elem.attrib["v"])
            if elem.attrib["k"]=="addr:housename": # add county after mapping
                if elem.attrib["v"]:
                    address["housename"]=elem.attrib["v"]
            if elem.attrib["k"]=="addr:housenumber": # add county after mapping
                if elem.attrib["v"]:
                    address["housenumber"]=elem.attrib["v"]
            if elem.attrib["k"]=="addr:neighbourhood": # add county after mapping
                if elem.attrib["v"]:
                    address["neighbourhood"]=elem.attrib["v"]
            if elem.attrib["k"]=="amenity": # add amenity to data
                if elem.attrib["v"]:
                    data["amenity"]=elem.attrib["v"]
            if elem.attrib["k"]=="name": # add amenity to data
                if elem.attrib["v"]:
                    data["name"]=elem.attrib["v"]
            if elem.attrib["k"] in ["Phone","phone"]:# adding phone number
                phone = clean_phone(elem.attrib["v"])
                if phone:
                    data["phone"]=phone

        
        if address != {}: # Append address if not empty
            address["state"]="TN" # Adding state field
            address["country"]="USA" #Adding country field
            data["address"]=address
        
        ref=[]
        for member in element.iter("member"):
            ref.append(member.attrib["ref"])
        if ref!=[] and element.tag=="relation":
            data["ref"]=ref
        
        for nd in element.iter("nd"):
            ref.append(nd.attrib["ref"])
        if ref!=[] and element.tag=="way":
            data["ref"]=ref 
    
        return data

#             try:
#                 if address["neighbourhood"]:
#                 pprint.pprint(data)
#                     break
#             except: KeyError

def data_to_json(file_in, file_out, pretty=False): # ++from the Case Study scripts++
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in,events=("start",)):
            el = shape_data(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def get_db(db_name): # initiating client and returns db ++from the Case Study scripts++
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def test():
	filename="nashville_tennessee.osm"
	
	file_in=open("nashville_tennessee.osm","r")
	file_out="cleaned_data_{0}.json".format(file_in.name[:10])

#     Generating JSON data from osm file
	cleaned_data=data_to_json(file_in, file_out)
	db=get_db("openstreetmap")
	result = db.nashville.insert(cleaned_data)

if __name__=="__main__":
	test()