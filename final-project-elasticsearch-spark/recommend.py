from elasticsearch import Elasticsearch
es = Elasticsearch()

def reverse_convert(s):
    return  [float(f.split("|")[1]) for f in s.split(" ")]


def fn_query(query_vec, q="*", cosine=False):
    
    return {
    "query": {
        "function_score": {
            "query" : { 
                "query_string": {
                    "query": q
                }
            },
            "script_score": {
                "script": {
                        "inline": "payload_vector_score",
                        "lang": "native",
                        "params": {
                            "field": "@model.factor",
                            "vector": query_vec,
                            "cosine" : cosine
                        }
                    }
            },
            "boost_mode": "replace"
        }
    }
}


def get_similar(the_id, q="*", num=10, index="demo", dt="movies"):
    
    response = es.get(index=index, doc_type=dt, id=the_id)
    src = response['_source']
    if '@model' in src and 'factor' in src['@model']:
        raw_vec = src['@model']['factor']
        query_vec = reverse_convert(raw_vec)
        q = fn_query(query_vec, q=q, cosine=True)
        results = es.search(index, dt, body=q)
        hits = results['hits']['hits']
        return src, hits[1:num+1]
    
    
def display_similar(the_id, q="*", num=10, index="demo", dt="movies"):
    
    movie, recs = get_similar(the_id, q, num, index, dt)
    
    print("Get similar movies for:")
    print(movie['title'])
    
    print("\nPeople who liked this movie also liked these: \n")
    
    for rec in recs:
        print(rec['_source']['title']  + " with score: "+ str(rec['_score']) + "\n")
        

#display_similar(2628, num=5)