import xml.etree.cElementTree as ET
from collections import defaultdict
import pprint

"""
Unzip the .bz2 file to .osm file and the set filename to the name of osm file.
"""


"""
Counting the number of various tags in the osm file.
This function is taken from the case study
"""
def count_tags(filename):
	tags = {}
# iterative parsing of tags
	with open (filename,"r") as f:
		for event, elem in ET.iterparse(f, events=("start",)):
			if elem.tag not in tags:
				tags[elem.tag] = 1
#increment for tags
			else:
				tags[elem.tag] += 1
	return tags

"""
The various attributes for each tag.
"""
def tag_attribs(filename):
	tag_attrib = defaultdict(set)
	with open (filename,"r") as f:
		for event, elem in ET.iterparse(f, events=("start",)):
			for e in elem.attrib:
				tag_attrib[elem.tag].add(e)
		return tag_attrib

"""
Function takes in dataname and data to write to "dataname.txt". in the current directory.
"""
def write_dict(data_name, data):
	with open("{0}.txt".format(data_name), 'w') as f_out:
		pprint.pprint(data,f_out)

def unique_keys(filename):
	kvalues=defaultdict(set)
	# parsing each tag at a  time, key values in the tag element is added to the corresponding dictionary.
	with open (filename,"r") as f:
		for event, element in ET.iterparse(f, events=("start",)):
			if element.tag in ["way", "node","relation"]:
				for elem in element.iter("tag"):
					kvalues[element.tag].add(elem.attrib["k"])
		return kvalues

"""
Auditing various fields in the data set to find inconsistencies. The audited fields are passed is written to a text file in the current directory.
"""
def audit_tags(filename):
    #Regular Expressions
	pincode=re.compile(r'^([0-9]{5})(?:-[0-9]{4})?$')
	phone=re.compile(r"^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$")
	street_name_re=re.compile(r"\b\S+\.?$", re.IGNORECASE)
    
    # fields in tags to be audited
	POST_FIELDS=["addr:postcode", "postal_code"]
	CITIES_FIELDS = ["addr:city", "is_in:city"]
	PHONE_FIELDS=["Phone","phone"]#,'contact:phone'],
                  #'telephone','communication:mobile_phone',
                  #'phone:local', 'disused:phone']
	STREET_FIELDS=['addr:street','destination:street']
	EXPECTED_STREET_NAMES = ["Avenue", "Court", "Lane", "Boulevard", "Drive", "Court", 
                             "Place","Road","Parkway","Circle","South","North","Highway",
                             "Trail","Terrace","Square", "Pike","Alley","Street","Trace",
                             "Bypass","Way","Fork", "Plaza","Broadway", "Loop", 
                             "Cove","Flagpole", "Foxborough", "Foxland", "Center", 
                             "Hollow","East","Heights", "Landing", "Springs",
                             "Hills", "Mission"]
	bad_pincodes=set()
	bad_states=set()
	cities=set()
	countries=set()
	bad_phones=set()
	good_phones=set()
	bad_street_names=defaultdict(set)
	county=set()
	amenities=set()
	housenames=set()
	housenums=set()
	neighbourhoods=set()
	names=set()

	with open (filename,"r") as f:
		for event, elem in ET.iterparse(f, events=("start",)):
			if elem.tag == "tag": 
				if elem.attrib["k"] in POST_FIELDS:
					code=elem.attrib["v"]
					m=pincode.match(code)
					if not m:
						bad_pincodes.add(code)
				if elem.attrib["k"]=="addr:state" and elem.attrib["v"]!="TN":
					bad_states.add(elem.attrib["v"])
				if elem.attrib["k"]=="amenity":
					amenities.add(elem.attrib["v"])
				if elem.attrib["k"]=="addr:housename":
					housenames.add(elem.attrib["v"])
				if elem.attrib["k"]=="addr:housenumber":
					housenums.add(elem.attrib["v"])
				if elem.attrib["k"]=="addr:neighbourhood":
					neighbourhoods.add(elem.attrib["v"])
				if elem.attrib["k"] in CITIES_FIELDS:
					cities.add(elem.attrib["v"])
				if elem.attrib["k"]=="addr:country":
					countries.add(elem.attrib["v"])
				if elem.attrib["k"]=="addr:county":
					county.add(elem.attrib["v"])
				if elem.attrib["k"]=="name":
					names.add(elem.attrib["v"])
				if elem.attrib["k"] in PHONE_FIELDS:
					phone_num=elem.attrib["v"].lstrip("+1- ")
					m=re.match(phone,phone_num)
					if m:
						good_phones.add(m.groups())
					if not m:
						bad_phones.add(phone_num)
				if elem.attrib["k"] in STREET_FIELDS:
					m=street_name_re.search(elem.attrib["v"])
					if m:
						street_type=m.group()
						if street_type not in EXPECTED_STREET_NAMES:
							bad_street_names[street_type].add(elem.attrib["v"])

	write_dict("bad_phones_nashvile",bad_phones)
	write_dict("good_phones_nashvile",good_phones)
	write_dict("bad_street_nashvile",bad_street_names)
	write_dict("countries_nashvile",countries)
	write_dict("amenity_nashvile",amenities)
	write_dict("housenames_nashvile",housenames)
	write_dict("housenums_nashvile",housenums)
	write_dict("cities_nashvile",cities)
	write_dict("bad_postcodes_nashvile",bad_pincodes)
	write_dict("bad_states_nashvile",bad_states)
	write_dict("neighbourhood_nashvile",neighbourhoods)
	write_dict("names_nashvile",names)


def test():
	filename="nashville_tennessee.osm"
	
	print("\nThe number of tags in osm file\t:")
	tag_count=count_tags(filename)
	pprint.pprint(tag_count)
	
	print("\nAttributes for each tag in osm file\t:")
	tag_attributes=tag_attribs(filename)
	pprint.pprint(tag_attributes)

	keys_in_tag=unique_keys(filename)
	write_dict("tag_keys_nashvile",keys_in_tag)

	audit_tags(filename)


if __name__=="__main__":
	test()