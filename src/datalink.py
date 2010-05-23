#! /usr/bin/python

import sys
import math
import gdbm
import time
import os
import os.path
import StringIO
import itertools

import pynappl
from rdflib.graph import Graph
from rdflib.namespace import Namespace
from rdflib.term import URIRef, Literal, BNode
from rdflib.parser import StringInputSource

sys.path.append('/home/iand/tmp/febrl-0.4.1/')
import comparison


class MyDataset:
  def __init__(self, store_name, selection_pattern, record_query, predicates):
    self.store_name = store_name
    self.store = pynappl.Store(store_name)
    self.selection_pattern = selection_pattern
    self.record_query = record_query
    self.predicates = predicates
    self.cache = None
    self.dataset_name = "%s_%s" % (self.store_name, hash(self.selection_pattern + "\t" + self.record_query))

  def read_records(self):
    
    index_filename = '%s_cache.db' % self.dataset_name

    if os.path.exists(index_filename) and os.path.isfile(index_filename):
      self.cache = gdbm.open(index_filename , 'r')
      return
    else:
      self.cache = gdbm.open(index_filename, 'c');

    batch_size = 2000
    offset = 0
    
    while True:
      while True:
        query = 'select ?resource {%s} limit %s offset %s' % (self.selection_pattern, batch_size, offset)
        print query
        (response, query_result) = self.store.select(query)
        if response.status in range (200,300):
          batch_size = math.trunc(batch_size * 1.2)
          break
        batch_size = math.trunc(batch_size * 0.5)
        if batch_size < 1:
          print "batch size reduced to zero"
          sys.exit()


      (resultheader, results) = query_result
      result_count = len(results)
      for result in results:
        resource_uri = str(result['resource'])
        print "Fetching %s" % resource_uri
        query = self.record_query.replace('?resource', '<%s>'%resource_uri)
        print query
        while True:
          response, body = self.store.sparql(query)
          if response.status in range (200,300):
            break
          
          time.sleep(5)  
          print "Retrying %s" % query
          
          
        self.cache[resource_uri] = body

      
      offset += batch_size
      if result_count < batch_size:
        break
    
  
  def get_records(self):
    if not self.cache:
      self.read_records()
      
    records = []
    my_resource_uri = self.cache.firstkey()
    while my_resource_uri != None:
      my_data = self.cache[my_resource_uri]
      my_graph = Graph()
      my_graph.parse(StringIO.StringIO(my_data))
      
      record_values = []
      for predicate in self.predicates:
        field_values = []
        for value in list(my_graph.objects(subject = URIRef(my_resource_uri), predicate = URIRef(predicate))):
          field_values.append(predicate +"\t" + value.encode('utf-8').lower())
        
        record_values.append(field_values)
        
      permuted_record_values = list(itertools.product(*record_values))
                  
      for permutation in permuted_record_values:
        record_data = {}
        for predicate_value in permutation:
          (predicate, value) = predicate_value.split("\t", 1)
          record_data[predicate] = value
          
        records.append( (my_resource_uri, record_data) )

      my_resource_uri = self.cache.nextkey(my_resource_uri)
    
    return records
    

class Matcher:

  def __init__(self, dataset_def1, dataset_def2, field_comparator_list):
  
    dataset1_predicates = set()
    dataset2_predicates = set()
    
    for (comparator, dataset1_predicate, dataset2_predicate, weight) in field_comparator_list:
      dataset1_predicates.add(dataset1_predicate)
      dataset2_predicates.add(dataset2_predicate)

    (dataset1_storename, dataset1_selection_pattern, dataset1_record_query) = dataset_def1
    (dataset2_storename, dataset2_selection_pattern, dataset2_record_query) = dataset_def2

    if not dataset1_record_query:
      dataset1_record_query = self.build_record_query(dataset1_predicates)

    if not dataset2_record_query:
      dataset2_record_query = self.build_record_query(dataset2_predicates)
    
    self.dataset1 = MyDataset(dataset1_storename, dataset1_selection_pattern, dataset1_record_query, dataset1_predicates) 
    self.dataset2 = MyDataset(dataset2_storename, dataset2_selection_pattern, dataset2_record_query, dataset2_predicates) 

    self.field_comparator_list = field_comparator_list
    
  def build_record_query(self, predicates):
    patterns = []
    optionals = []
    index = 0
    for predicate in predicates:
      optionals.append('optional {?resource <%s> ?v%s .} ' % (predicate, index))
      patterns.append('<%s> ?v%s ' % (predicate, index))
      index += 1
    
    return 'construct {?resource %s .} { %s }' % ( '; '.join(patterns),  ' '.join(optionals) )
    
    
  def match(self, match_predicate='http://www.w3.org/2002/07/owl#sameAs'):
    my_records = self.dataset1.get_records()
    other_records = self.dataset2.get_records()
  
    for my_resource_uri, my_data in my_records:
      max_score = 0
      max_score_uri = ''
      
      for other_resource_uri, other_data in other_records:
            
        weight_vector = self.compare( my_data, other_data )
        score = 0
        for i in range(0, len(weight_vector)):
          score += weight_vector[i]
        
        
        if score > 0:
          #print "%s compared with %s gives %s: %s" %( my_resource_uri, other_resource_uri, score, weight_vector )
          if score >= max_score:
            max_score = score
            max_score_uri = other_resource_uri
      
      if max_score> 0:  
        print ""
        print "# Matched with a score of %s" % max_score
        print "<%s> <%s> <%s> ." %( my_resource_uri, match_predicate, max_score_uri )
      else:
        print ""
        print "# Could not match %s" % my_resource_uri


  def compare(self, rec1, rec2):
    weight_vector = []


    for (comparator, dataset1_predicate, dataset2_predicate, weight) in self.field_comparator_list:
      val1 = rec1[dataset1_predicate]
      val2 = rec2[dataset2_predicate]

      if (val1.isalpha()):
        val1 = val1.lower()

      if (val2.isalpha()):  
        val2 = val2.lower()

      w = comparator.compare(val1,val2) * weight
      weight_vector.append(w)

    return weight_vector
  
  


astronaut_matcher = Matcher(
                            ['space', '?resource a <http://xmlns.com/foaf/0.1/Person>', None],
                            ['dbpedia', '?resource a <http://dbpedia.org/ontology/Astronaut>', None],
                           [   
                            (comparison.FieldComparatorExactString(), 'http://xmlns.com/foaf/0.1/name', 'http://xmlns.com/foaf/0.1/name', 10),
                            (comparison.FieldComparatorContainsString(), 'http://xmlns.com/foaf/0.1/name', 'http://xmlns.com/foaf/0.1/name', 2),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://xmlns.com/foaf/0.1/name', 'http://xmlns.com/foaf/0.1/name', 1),
                            (comparison.FieldComparatorExactString(), 'http://xmlns.com/foaf/0.1/name', 'http://www.w3.org/2000/01/rdf-schema#label', 5),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://xmlns.com/foaf/0.1/name', 'http://www.w3.org/2000/01/rdf-schema#label', 1),
                           ],
                           
                          )

#astronaut_matcher.match()



mission_matcher = Matcher(
                            ['space', '?resource a <http://purl.org/net/schemas/space/Mission>', None],
                            ['dbpedia', '?resource a <http://dbpedia.org/ontology/SpaceMission>', None],
                           [   
                            (comparison.FieldComparatorExactString(), 'http://purl.org/dc/elements/1.1/title', 'http://xmlns.com/foaf/0.1/name', 10),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://purl.org/dc/elements/1.1/title', 'http://xmlns.com/foaf/0.1/name', 1),
                            (comparison.FieldComparatorExactString(), 'http://purl.org/dc/elements/1.1/title', 'http://www.w3.org/2000/01/rdf-schema#label', 5),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://purl.org/dc/elements/1.1/title', 'http://www.w3.org/2000/01/rdf-schema#label', 1),
                           ],
                           
                          )

#mission_matcher.match()


launchsite_matcher = Matcher(
                            ['space', '?resource a <http://purl.org/net/schemas/space/LaunchSite>', None],
                            ['dbpedia', '?resource a <http://dbpedia.org/ontology/Country>', None],
                           [   
                            (comparison.FieldComparatorExactString(), 'http://purl.org/net/schemas/space/country', 'http://xmlns.com/foaf/0.1/name', 10),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://purl.org/net/schemas/space/country', 'http://xmlns.com/foaf/0.1/name', 1),
                            (comparison.FieldComparatorExactString(), 'http://purl.org/net/schemas/space/country', 'http://www.w3.org/2000/01/rdf-schema#label', 5),
                            (comparison.FieldComparatorJaro(threshold=0.9), 'http://purl.org/net/schemas/space/country', 'http://www.w3.org/2000/01/rdf-schema#label', 1),
                           ],
                           
                          )

launchsite_matcher.match()

#space_spacecraft = MyDataset('space_spacecraft','space', [ ('name', 'http://xmlns.com/foaf/0.1/name' ), ] )
#space_spacecraft.selection_pattern = '?resource a <http://purl.org/net/schemas/space/Spacecraft>'
#space_spacecraft.read_records()




#dbpedia_spacecraft = MyDataset('dbpedia_spacecraft', 'dbpedia', [ ('name', 'http://xmlns.com/foaf/0.1/name' ),
#                                                      ('label', 'http://www.w3.org/2000/01/rdf-schema#label' )
#                                                      ] )
#dbpedia_spacecraft.selection_pattern = '?resource a <http://dbpedia.org/ontology/SpaceMission>'
#dbpedia_spacecraft.read_records()


#spacecraft_comp = comparison.RecordComparator(space_spacecraft, dbpedia_spacecraft, [ 
#                                                    (comparison.FieldComparatorExactString(), 'title', 'name'),
#                                                    (comparison.FieldComparatorJaro(threshold=0.9), 'title', 'name'),
#                                                    (comparison.FieldComparatorExactString(), 'title', 'label'),
#                                                    (comparison.FieldComparatorJaro(threshold=0.9), 'title', 'label'),
#                                                   ] )

#space_spacecraft.match_all(spacecraft_comp, dbpedia_spacecraft, [10, 1, 5, 1])

#
#
#
