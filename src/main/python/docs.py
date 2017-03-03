#/bin/python

f1_in = 'promoted_content.csv'
f2_in = 'documents_meta.csv'
f3_in = 'documents_categories.csv'
f4_in = 'documents_topics.csv'
f5_in = 'documents_entities.csv'

f1 = open(f1_in, 'r')
f2 = open(f2_in, 'r')
f3 = open(f1_in, 'r')
f4 = open(f1_in, 'r')
f5 = open(f1_in, 'r')

doc_id_f1 = {}
doc_id_f2 = {}
doc_id_f3 = {}
doc_id_f4 = {}
doc_id_f5 = {}


for row in f1:
  tokens = row.strip('\n').split(',')
  doc_id = tokens[1]
  if doc_id != 'document_id':
    if doc_id in doc_id_f1:
      doc_id_f1[doc_id] = doc_id_f1[doc_id] + 1
    else:
      doc_id_f1[doc_id] = 1

for row in f2:
  tokens = row.strip('\n').split(',')
  doc_id = tokens[0]
  if doc_id != 'document_id':
    if doc_id in doc_id_f2:
      doc_id_f2[doc_id] = doc_id_f2[doc_id] + 1
    else:
      doc_id_f2[doc_id] = 1

for row in f3:
  tokens = row.strip('\n').split(',')
  doc_id = tokens[0]
  if doc_id != 'document_id':
    if doc_id in doc_id_f3:
      doc_id_f3[doc_id] = doc_id_f3[doc_id] + 1
    else:
      doc_id_f3[doc_id] = 1

for row in f4:
  tokens = row.strip('\n').split(',')
  doc_id = tokens[0]
  if doc_id != 'document_id':
    if doc_id in doc_id_f4:
      doc_id_f4[doc_id] = doc_id_f4[doc_id] + 1
    else:
      doc_id_f4[doc_id] = 1

for row in f5:
  tokens = row.strip('\n').split(',')
  doc_id = tokens[0]
  if doc_id != 'document_id':
    if doc_id in doc_id_f5:
      doc_id_f5[doc_id] = doc_id_f5[doc_id] + 1
    else:
      doc_id_f5[doc_id] = 1

print 'length of promoted_content docids= ',  len(doc_id_f1)
print 'length of doc_meta docids= ', len(doc_id_f2)
print 'length of doc_categories docids= ', len(doc_id_f3)
print 'length of doc_topics docids= ', len(doc_id_f4)
print 'length of doc_entities docids= ', len(doc_id_f5)
